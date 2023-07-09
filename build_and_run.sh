podman stop tn_collection
podman rm tn_collection
podman stop tn_refresh
podman rm tn_refresh
podman rmi localhost/collection_app_collection:latest
podman rmi localhost/refresh_app_refresh:latest
podman-compose up -d