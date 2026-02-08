import unittest
from unittest.mock import Mock, MagicMock, patch
import json
from general_functions import sync_bls_files_to_s3


class TestSyncBlsFilesToS3(unittest.TestCase):
    """Unit tests for sync_bls_files_to_s3 function"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_s3_client = Mock()
        self.bucket_name = "test-bucket"
        self.bucket_directory = "bls_data"
        self.manifest_log_file = "bls_data/file_log.json"
        
        # Sample source file list
        self.source_files = [
            {
                "file_name": "pr.data.0.Current",
                "last_updated": "2026-02-08T10:00:00Z",
                "full_url": "https://download.bls.gov/pub/time.series/pr/pr.data.0.Current"
            },
            {
                "file_name": "pr.data.1.AllData",
                "last_updated": "2026-02-08T10:00:00Z",
                "full_url": "https://download.bls.gov/pub/time.series/pr/pr.data.1.AllData"
            }
        ]

    def test_sync_bls_files_new_files_upload(self):
        """Test uploading new files when manifest doesn't exist"""
        # Mock S3 operations - manifest doesn't exist
        with patch('general_functions.s3_object_exists', return_value=False):
            with patch('general_functions.requests.Session') as mock_session_class:
                mock_session = Mock()
                mock_session_class.return_value = mock_session
                
                # Mock the HTTP response
                mock_response = Mock()
                mock_response.content = b"file content"
                mock_response.headers = {"Content-Type": "text/plain"}
                mock_response.__enter__ = Mock(return_value=mock_response)
                mock_response.__exit__ = Mock(return_value=None)
                mock_session.get.return_value = mock_response
                
                result = sync_bls_files_to_s3(
                    self.source_files,
                    self.mock_s3_client,
                    self.bucket_name,
                    self.bucket_directory,
                    self.manifest_log_file
                )
        
        # Verify result
        self.assertEqual(result["uploaded"], 2)
        self.assertEqual(result["deleted"], 0)
        
        # Verify S3 put_object was called for each file
        self.assertEqual(self.mock_s3_client.put_object.call_count, 3)  # 2 files + manifest

    def test_sync_bls_files_skip_unchanged_files(self):
        """Test that unchanged files are not re-uploaded"""
        # Mock existing manifest in S3
        existing_manifest = self.source_files.copy()
        self.mock_s3_client.head_object.return_value = {}
        self.mock_s3_client.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=json.dumps(existing_manifest).encode("utf-8")))
        }
        
        with patch('general_functions.requests.Session') as mock_session_class:
            mock_session = Mock()
            mock_session_class.return_value = mock_session
            
            result = sync_bls_files_to_s3(
                self.source_files,
                self.mock_s3_client,
                self.bucket_name,
                self.bucket_directory,
                self.manifest_log_file
            )
        
        # Verify no files were uploaded (only manifest updated)
        self.assertEqual(result["uploaded"], 0)
        self.assertEqual(result["deleted"], 0)
        
        # put_object should only be called once for the manifest update
        self.assertEqual(self.mock_s3_client.put_object.call_count, 1)

    def test_sync_bls_files_delete_removed_files(self):
        """Test that files removed from source are deleted from S3"""
        # Mock existing manifest with extra file
        existing_manifest = [
            {
                "file_name": "pr.data.0.Current",
                "last_updated": "2026-02-08T10:00:00Z",
                "full_url": "https://download.bls.gov/pub/time.series/pr/pr.data.0.Current"
            },
            {
                "file_name": "pr.data.1.AllData",
                "last_updated": "2026-02-08T10:00:00Z",
                "full_url": "https://download.bls.gov/pub/time.series/pr/pr.data.1.AllData"
            },
            {
                "file_name": "pr.data.2.OldFile",
                "last_updated": "2026-01-01T10:00:00Z",
                "full_url": "https://download.bls.gov/pub/time.series/pr/pr.data.2.OldFile"
            }
        ]
        
        self.mock_s3_client.head_object.return_value = {}
        self.mock_s3_client.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=json.dumps(existing_manifest).encode("utf-8")))
        }
        
        with patch('general_functions.requests.Session') as mock_session_class:
            mock_session = Mock()
            mock_session_class.return_value = mock_session
            
            result = sync_bls_files_to_s3(
                self.source_files,  # Only has 2 files, missing the old one
                self.mock_s3_client,
                self.bucket_name,
                self.bucket_directory,
                self.manifest_log_file
            )
        
        # Verify the old file was deleted
        self.assertEqual(result["deleted"], 1)
        
        # delete_object should be called once for the old file
        self.mock_s3_client.delete_object.assert_called()

    def test_sync_bls_files_upload_updated_files(self):
        """Test that files with newer timestamps are re-uploaded"""
        # Mock existing manifest with older timestamp
        existing_manifest = [
            {
                "file_name": "pr.data.0.Current",
                "last_updated": "2026-02-01T10:00:00Z",  # Older timestamp
                "full_url": "https://download.bls.gov/pub/time.series/pr/pr.data.0.Current"
            }
        ]
        
        self.mock_s3_client.head_object.return_value = {}
        self.mock_s3_client.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=json.dumps(existing_manifest).encode("utf-8")))
        }
        
        # Source has newer timestamp
        source_files_updated = [
            {
                "file_name": "pr.data.0.Current",
                "last_updated": "2026-02-08T10:00:00Z",  # Newer timestamp
                "full_url": "https://download.bls.gov/pub/time.series/pr/pr.data.0.Current"
            }
        ]
        
        with patch('general_functions.requests.Session') as mock_session_class:
            mock_session = Mock()
            mock_session_class.return_value = mock_session
            
            # Mock the HTTP response
            mock_response = Mock()
            mock_response.content = b"updated file content"
            mock_response.headers = {"Content-Type": "text/plain"}
            mock_response.__enter__ = Mock(return_value=mock_response)
            mock_response.__exit__ = Mock(return_value=None)
            mock_session.get.return_value = mock_response
            
            result = sync_bls_files_to_s3(
                source_files_updated,
                self.mock_s3_client,
                self.bucket_name,
                self.bucket_directory,
                self.manifest_log_file
            )
        
        # Verify the updated file was uploaded
        self.assertEqual(result["uploaded"], 1)
        self.assertEqual(result["deleted"], 0)


class TestGeneralFunctions(unittest.TestCase):
    """Additional tests for other functions in general_functions module"""

    def test_load_config(self):
        """Test configuration loading"""
        from general_functions import load_config
        
        # Test with actual config.yaml file
        config = load_config("config.yaml")
        
        self.assertIn("bureau_labor_statistics_connection_info", config)
        self.assertIn("data_usa_connection_info", config)
        self.assertIn("aws_s3_connection_info", config)

    def test_build_s3_client(self):
        """Test S3 client creation"""
        from general_functions import build_s3_client
        
        with patch('general_functions.boto3.client') as mock_boto3_client:
            build_s3_client("us-east-2")
            
            mock_boto3_client.assert_called_once()
            call_args = mock_boto3_client.call_args
            self.assertEqual(call_args[1]["region_name"], "us-east-2")


if __name__ == "__main__":
    unittest.main()
