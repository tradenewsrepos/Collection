podman stop tn_collection
podman rm tn_collection
podman rmi localhost/collection_app_collection:latest
podman-compose up -d