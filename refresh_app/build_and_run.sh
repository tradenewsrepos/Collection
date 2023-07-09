podman stop tn_refresh
podman rm tn_refresh
podman rmi localhost/refresh_app_refresh:latest
podman-compose up -d