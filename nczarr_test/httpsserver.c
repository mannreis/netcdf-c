#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#define close closesocket
#else
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#endif

#include <openssl/ssl.h>
#include <openssl/err.h>

#define BUF_SIZE 4096

void send_404(SSL *ssl) {
    const char *resp =
        "HTTP/1.1 404 Not Found\r\n"
        "Content-Length: 13\r\n"
        "Content-Type: text/plain\r\n\r\n"
        "404 Not Found";
    SSL_write(ssl, resp, (int)strlen(resp));
}

void serve_file(SSL *ssl, const char *root, const char *path, int info) {
    char fullpath[1024];
    snprintf(fullpath, sizeof(fullpath), "%s%s", root, path);

    FILE *f = fopen(fullpath, "rb");
    if (!f) {
        send_404(ssl);
        return;
    }

    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    rewind(f);

    char header[256];
    snprintf(header, sizeof(header),
        "HTTP/1.1 200 OK\r\n"
        "Content-Length: %ld\r\n"
        "Content-Type: application/octet-stream\r\n\r\n",
        size);

    SSL_write(ssl, header, (int)strlen(header));

    if (!info)
    {
        char buf[BUF_SIZE];
        size_t n;
        while ((n = fread(buf, 1, sizeof(buf), f)) > 0) {
            SSL_write(ssl, buf, (int)n);
        }
    }

    fclose(f);
}

int main(int argc, char **argv) {
    if (argc != 5) {
        fprintf(stderr,
            "Usage: %s <port> <directory> <cert.pem> <key.pem>\n", argv[0]);
        return 1;
    }

#ifdef _WIN32
    WSADATA wsa;
    WSAStartup(MAKEWORD(2,2), &wsa);
#endif

    const char *port = argv[1];
    const char *root = argv[2];

    for (const char * c = port; *c!= '\0'; c++){
        if (*c < '0' || *c > '9'){
            fprintf(stderr, "Invalid port provided!\n");
            return 2;
        }
    }

    SSL_library_init();
    SSL_load_error_strings();

    SSL_CTX *ctx = SSL_CTX_new(TLS_server_method());
    SSL_CTX_use_certificate_file(ctx, argv[3], SSL_FILETYPE_PEM);
    SSL_CTX_use_PrivateKey_file(ctx, argv[4], SSL_FILETYPE_PEM);

    int server = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)atoi(port));
    addr.sin_addr.s_addr = INADDR_ANY;

    bind(server, (struct sockaddr *)&addr, sizeof(addr));
    listen(server, 10);

    printf("Serving directory %s via HTTPS on port %s\n", root, port);

    while (1) {
        int client = accept(server, NULL, NULL);

        SSL *ssl = SSL_new(ctx);
        SSL_set_fd(ssl, client);

        if (SSL_accept(ssl) > 0) {
            char req[1024] = {0};
            SSL_read(ssl, req, sizeof(req) - 1);

            char method[8], path[512];
            sscanf(req, "%7s %511s", method, path);

            fprintf(stderr, "Received: %-1024s\n",req);

            int info = (strcmp(method, "HEAD") == 0);
            if (strcmp(method, "GET") == 0 || info )
            {
                serve_file(ssl, root, path, info);
            }
            else
            {
                send_404(ssl);
            }
        }

        SSL_shutdown(ssl);
        SSL_free(ssl);
        close(client);
    }

    SSL_CTX_free(ctx);
}
