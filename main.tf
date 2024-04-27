provider "aws" {
 region = "us-east-1"
}

resource "aws_s3_bucket" "kafka-stock-price" {
 bucket = "kafka-stock-price"
}

# will use it for athena 
resource "aws_s3_bucket" "stock-s3-temp" {
 bucket = "stock-s3-temp"
}

resource "aws_instance" "kafka_instance" {
 ami           = "ami-04b70fa74e45c3917"
 instance_type = "t2.micro"

 vpc_security_group_ids = [aws_security_group.kafka_sg.id]

 tags = {
   Name = "Kafka Instance"
 }
}

resource "aws_security_group" "kafka_sg" {
 name        = "kafka-security-group"
 description = "Allow inbound traffic on port 9092"

 ingress {
   from_port   = 9092
   to_port     = 9092
   protocol    = "tcp"
   cidr_blocks = ["0.0.0.0/0"]
 }
}

resource "aws_glue_catalog_database" "glue_database" {
 name = "stock-database"
}