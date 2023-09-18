resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = "example_dataset"
  friendly_name               = "test"
  description                 = "This is a test description"
  location                    = "EU"
  default_table_expiration_ms = 3600000

  access {
    role          = "OWNER"
    user_by_email = "terraform-bq@gcp-demo-dev-397415.iam.gserviceaccount.com"
  }

  access {
    role   = "READER"
    domain = "hashicorp.com"
  }
}