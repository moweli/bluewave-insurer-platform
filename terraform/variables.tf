variable "resource_group_name" {
  type    = string
  default = "bluewave-rg"
}
variable "location" {
  type    = string
  default = "uksouth"
}
variable "storage_account_name" {
  type    = string
  default = "bluewaveplatformsa"  # must be globally unique => modify as needed
}
