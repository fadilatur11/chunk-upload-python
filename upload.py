import boto3
import os

# Konfigurasi AWS
AWS_ACCESS_KEY = "XXXXX"
AWS_SECRET_KEY = "XXXXXXX"
AWS_BUCKET_NAME = "XXXXXXXXX"
AWS_REGION = "XXXXXXXXX"  # Sesuaikan dengan region bucket

def upload_file_to_s3_multipart(file_path, bucket_name, s3_folder="", chunk_size_mb=50):
    """
    Mengunggah file besar ke S3 menggunakan multipart upload dengan path yang bisa di-custom.
    :param file_path: Lokasi file yang akan diunggah
    :param bucket_name: Nama bucket S3
    :param s3_folder: Folder tujuan di dalam S3 (contoh: "backup/files/")
    :param chunk_size_mb: Ukuran tiap chunk dalam MB (default: 50MB)
    """
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )

    # Nama file di dalam bucket (menggunakan folder jika diberikan)
    file_name = os.path.basename(file_path)
    s3_key = f"{s3_folder.rstrip('/')}/{file_name}" if s3_folder else file_name

    file_size = os.path.getsize(file_path)
    chunk_size = chunk_size_mb * 1024 * 1024  # Konversi MB ke byte
    num_parts = (file_size // chunk_size) + (1 if file_size % chunk_size else 0)

    print(f"Uploading {file_path} to s3://{bucket_name}/{s3_key} in {num_parts} parts...")

    # Inisialisasi multipart upload
    multipart_upload = s3_client.create_multipart_upload(Bucket=bucket_name, Key=s3_key)
    upload_id = multipart_upload["UploadId"]
    parts = []

    try:
        with open(file_path, "rb") as file:
            for part_number in range(1, num_parts + 1):
                chunk = file.read(chunk_size)
                response = s3_client.upload_part(
                    Bucket=bucket_name,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=chunk
                )
                parts.append({"PartNumber": part_number, "ETag": response["ETag"]})
                print(f"Uploaded part {part_number}/{num_parts}")

        # Menyelesaikan multipart upload
        s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=s3_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts}
        )
        print(f"Upload completed successfully! File available at: s3://{bucket_name}/{s3_key}")

    except Exception as e:
        print(f"Upload failed: {e}")
        s3_client.abort_multipart_upload(Bucket=bucket_name, Key=s3_key, UploadId=upload_id)

# Contoh penggunaan
if __name__ == "__main__":
    file_path = "200MB.zip"  # Sesuaikan dengan file yang ingin diupload
    s3_folder = "test/"  # Path tujuan di dalam S3
    upload_file_to_s3_multipart(file_path, AWS_BUCKET_NAME, s3_folder, chunk_size_mb=50)
