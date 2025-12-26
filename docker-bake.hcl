variable "REGISTRY" {
  default = "ghcr.io"
}

variable "REPO" {
  default = "local/notion-sync"
}

variable "TAGS" {
  default = ["main"]
}

group "default" {
  targets = ["notion-sync"]
}

target "notion-sync" {
  context = "."
  dockerfile = "Dockerfile"
  platforms = ["linux/amd64", "linux/arm64"]
  tags = formatlist("${REGISTRY}/${REPO}:%s", TAGS)
}
