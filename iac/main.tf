resource "aws_s3_bucket" "datalake" {
    bucket = "${var.base_bucket_name}-${var.ambiente}-${var.numero_da_conta}"
    acl = "private"
    server_side_encryption_configuration {
        rule {
            apply_server_side_encryption_by_default{
                sse_algorithm = "AES256"
            }
        }
    }

    tags = {
        EDC = "BOOTCAMP"
    }    
}

resource "aws_s3_bucket_object" "codigo_spark" {

    bucket = aws_s3_bucket.datalake.id
    key = "emr-code/pyspark/job_spark_from_tf.py"
    acl = "private"
    source = "to_parquet2.py"
    etag = filemd5("to_parquet2.py")
}

provider "aws"{
    region = "sa-east-1"
    shared_config_files      = ["/home/penalva/.aws/config"]
    shared_credentials_files = ["/home/penalva/.aws/credentials"]
    profile                  = "awsadm"
}

terraform {
  backend "s3" {
    bucket = "terraform-state-igti-edc-penalvd"
    key = "state/igti/edc/mod1/terraform.tfstate"
    region = "sa-east-1"
  }
}