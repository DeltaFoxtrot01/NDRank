
resource "google_secret_manager_secret" "github_token" {
  secret_id = "github-token"

  labels = {
    label = "github-secret-${var.tag}"
  }

  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }
}

resource "google_secret_manager_secret_version" "github-token-version" {
  secret = google_secret_manager_secret.github_token.id
  secret_data = var.github_token
}

resource "google_cloudbuild_trigger" "standard_build" {
  name = "test-trigger"

  github {
    owner = "penedocapital"
    name = "pweather"
    push {
      branch = "master"
    }
  }

  filename = "cloudbuild.yaml"
}

resource "google_cloudbuild_trigger" "pweather_distributor_build" {
  name = "pweather-distributor-trigger"

  github {
    owner = "penedocapital"
    name = "pweather-distributor"
    push {
      branch = "main"
    }
  }

  filename = "cloudbuild.yaml"
}

resource "google_cloudbuild_trigger" "pweather_resolution_reducer_build" {
  name = "pweather-resolution-reducer-trigger"

  github {
    owner = "penedocapital"
    name = "pweather-resolution-reducer"
    push {
      branch = "main"
    }
  }

  filename = "cloudbuild.yaml"
}

resource "google_cloudbuild_trigger" "pweather_downloader_build" {
  name = "pweather-downloader-trigger"

  github {
    owner = "penedocapital"
    name = "pweather-downloader"
    push {
      branch = "main"
    }
  }

  filename = "cloudbuild.yaml"
}

resource "google_cloudbuild_trigger" "pweather_cluster_build" {
  name = "pweather-cluster-trigger"

  github {
    owner = "penedocapital"
    name = "pweather-cluster-nodes"
    push {
      branch = "main"
    }
  }

  filename = "cloudbuild.yaml"
}

resource "google_cloudbuild_trigger" "pweather_brute_force_ndrank" {
  name = "pweather-brute-force-ndrank"

  github {
    owner = "penedocapital"
    name = "pweather-brute-force-ndrank"
    push {
      branch = "main"
    }
  }

  substitutions = {
    _REGION = var.zone
  }

  filename = "cloudbuild.yaml"
}
