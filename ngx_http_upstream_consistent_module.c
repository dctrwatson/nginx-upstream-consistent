/*
 * Upstream provider for memcached_pass impletementing the weighted ketama algorithm 
 * 
 * Copyright (C) John Watson
 *
 * This module can be distributed under the same terms as Nginx itself.
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_md5.h>

#include <math.h>

typedef struct {
    ngx_http_upstream_server_t  *server;
    ngx_uint_t                  weight;
} ngx_http_upstream_consistent_peer_t;

typedef struct {
    ngx_uint_t index;
    ngx_uint_t value;
} ngx_http_upstream_consistent_continuum_item_t;
    
typedef struct {
    ngx_uint_t  hash;
    ngx_str_t   key;
    ngx_http_upstream_consistent_peer_t           *peers;
    ngx_http_upstream_consistent_continuum_item_t *continuum;
    ngx_uint_t                              continuum_points_counter; 
} ngx_http_upstream_consistent_peer_data_t;

typedef struct {
    ngx_http_upstream_consistent_peer_t           *peers;
    ngx_http_upstream_consistent_continuum_item_t *continuum;
    ngx_uint_t                              continuum_points_counter;
    ngx_uint_t                              continuum_count;
} ngx_http_upstream_consistent_data_t;

#define POINTS_PER_SERVER       160                     // MEMCACHED_POINTS_PER_SERVER_KETAMA
#define CONTINUUM_ADDITION      10                      // MEMCACHED_CONTINUUM_ADDITION
#define CONTINUUM_SIZE          POINTS_PER_SERVER*100   // MEMCACHED_CONTINUUM_SIZE
#define MAX_HOST_SORT_LENGTH    86                      // MEMCACHED_MAX_HOST_SORT_LENGTH

static ngx_int_t ngx_http_upstream_init_consistent_peer(ngx_http_request_t *r,
    ngx_http_upstream_srv_conf_t *us);
static ngx_int_t ngx_http_upstream_get_consistent_peer(ngx_peer_connection_t *pc,
    void *data);
static void ngx_http_upstream_free_consistent_peer(ngx_peer_connection_t *pc,
    void *data, ngx_uint_t state);
static char *ngx_http_upstream_consistent(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static ngx_int_t ngx_http_upstream_init_consistent(ngx_conf_t *cf, 
    ngx_http_upstream_srv_conf_t *us);
static int ngx_http_upstream_consistent_continuum_item_cmp(const void *t1, const void *t2);
static u_char* ngx_http_upstream_consistent_md5_hash(u_char *result, const u_char *key, size_t key_length);
static ngx_uint_t ngx_http_upstream_consistent_ketama_hash(const u_char *key, 
    size_t key_length, ngx_uint_t alignment);

static ngx_command_t  ngx_http_upstream_consistent_commands[] = {
    { ngx_string("consistent"),
      NGX_HTTP_UPS_CONF|NGX_CONF_NOARGS,
      ngx_http_upstream_consistent,
      0,
      0,
      NULL },

      ngx_null_command
};


static ngx_http_module_t  ngx_http_upstream_consistent_module_ctx = {
    NULL,                                  /* preconfiguration */
    NULL,                                  /* postconfiguration */

    NULL,                                  /* create main configuration */
    NULL,                                  /* init main configuration */

    NULL,                                  /* create server configuration */
    NULL,                                  /* merge server configuration */

    NULL,                                  /* create location configuration */
    NULL                                   /* merge location configuration */
};


ngx_module_t  ngx_http_upstream_consistent_module = {
    NGX_MODULE_V1,
    &ngx_http_upstream_consistent_module_ctx,    /* module context */
    ngx_http_upstream_consistent_commands,       /* module directives */
    NGX_HTTP_MODULE,                       /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    NULL,                                  /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


static ngx_int_t
ngx_http_upstream_init_consistent(ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *us)
{
    ngx_uint_t                              i, j, continuum_index, pointer_index, pointer_per_server, pointer_counter, value;
    ngx_http_upstream_server_t              *server;
    ngx_http_upstream_consistent_peer_t           *peers;
    ngx_http_upstream_consistent_data_t           *ucd;
    ngx_http_upstream_consistent_continuum_item_t *continuum;
    uint64_t                                total_weight;

    us->peer.init = ngx_http_upstream_init_consistent_peer;
    
    ucd = us->peer.data;

    if (!us->servers) {
        return NGX_ERROR;
    }

    server = us->servers->elts;

    peers = ngx_pcalloc(cf->pool, sizeof(ngx_http_upstream_consistent_peer_t) * us->servers->nelts);

    if (peers == NULL) {
        return NGX_ERROR;
    }

    total_weight = 0;
    for (i = 0; i < us->servers->nelts; i++) {
        peers[i].server = &server[i];
        total_weight += server[i].weight;
    }

    ucd->continuum_count = us->servers->nelts + CONTINUUM_ADDITION;
    
    continuum = ngx_pcalloc(cf->pool, sizeof(ngx_http_upstream_consistent_continuum_item_t) 
                            * ucd->continuum_count * POINTS_PER_SERVER);
    
    if (continuum == NULL) {
        return NGX_ERROR;
    }

    pointer_counter = continuum_index = 0;

    for (i = 0; i < us->servers->nelts; ++i) {
        float pct = (float)peers[i].server->weight / (float)total_weight;
        pointer_per_server = (ngx_uint_t) ((floorf((float) (pct * POINTS_PER_SERVER / 4 * (float) us->servers->nelts + 0.0000000001))) * 4);
        
        // 4 == pointer_per_hash
        for (pointer_index = 1; pointer_index <= pointer_per_server / 4; pointer_index++) {
            u_char *sort_host;

            sort_host = ngx_pcalloc(cf->pool, MAX_HOST_SORT_LENGTH);
            if (sort_host == NULL) {
                return NGX_ERROR;
            }

            if (ngx_strstr(peers[i].server->addrs->name.data, ":11211") != NULL) {
                 peers[i].server->addrs->name.len -= 6;
                 peers[i].server->addrs->name.data[peers[i].server->addrs->name.len] = '\0';
            }

            ngx_snprintf(sort_host, MAX_HOST_SORT_LENGTH, "%V-%ui%Z", &peers[i].server->addrs->name, pointer_index-1);

            // 4 == pointer_per_hash
            for (j = 0; j < 4; j++) {
                value = ngx_http_upstream_consistent_ketama_hash(sort_host, ngx_strlen(sort_host), j);
                continuum[continuum_index].index = i;
                continuum[continuum_index++].value = value;
            }
        }

        pointer_counter += pointer_per_server;
    }

    ngx_qsort(continuum, pointer_counter, sizeof(ngx_http_upstream_consistent_continuum_item_t), ngx_http_upstream_consistent_continuum_item_cmp);

    ucd->peers = peers;

    ucd->continuum_points_counter = pointer_counter;
    ucd->continuum = continuum;

    /*
    for (i = 0; i < continuum_index; i++) {
        ngx_log_error(NGX_LOG_WARN, cf->log, 0, "upstream_consistent: %i", continuum[i].value);
    }
    */

    return NGX_OK;
}


static ngx_int_t
ngx_http_upstream_init_consistent_peer(ngx_http_request_t *r,
    ngx_http_upstream_srv_conf_t *us)
{
    ngx_http_upstream_consistent_peer_data_t     *ucpd;
    ngx_buf_t *b;
    
    ngx_http_upstream_consistent_data_t *ucd = us->peer.data;
    
    ucpd = ngx_pcalloc(r->pool, sizeof(ngx_http_upstream_consistent_peer_data_t));
    if (ucpd == NULL) {
        return NGX_ERROR;
    }

    r->upstream->peer.data = ucpd;

    r->upstream->peer.free = ngx_http_upstream_free_consistent_peer;
    r->upstream->peer.get = ngx_http_upstream_get_consistent_peer;
    r->upstream->peer.tries = 1;

    b = r->upstream->request_bufs->buf;

    ucpd->key.len = b->end-b->start-sizeof("get ")-sizeof(CRLF)+3;

    ucpd->key.data = ngx_pcalloc(r->pool, ucpd->key.len);

    if (ucpd->key.data == NULL) {
        return NGX_ERROR;
    }

    ngx_cpystrn(ucpd->key.data, b->start+4, ucpd->key.len);
    
    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "upstream_consistent: key \"%V\"", &ucpd->key);

    ucpd->hash = ngx_http_upstream_consistent_ketama_hash(ucpd->key.data, ucpd->key.len-1, 0);

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "upstream_consistent: hash %ui", ucpd->hash);

    ucpd->continuum = ucd->continuum;
    ucpd->continuum_points_counter = ucd->continuum_points_counter;
    ucpd->peers = ucd->peers;

    return NGX_OK;
}


static ngx_int_t
ngx_http_upstream_get_consistent_peer(ngx_peer_connection_t *pc, void *data)
{
    ngx_http_upstream_consistent_continuum_item_t *begin, *end, *left, *right, *middle;
    ngx_http_upstream_consistent_peer_data_t  *ucpd = data;
    ngx_http_upstream_consistent_peer_t       *peer;

    pc->cached = 0;
    pc->connection = NULL;

    begin = left = ucpd->continuum;
    end = right = ucpd->continuum + ucpd->continuum_points_counter;

    while (left < right) {
        middle = left + (right - left) / 2;
        if (middle->value < ucpd->hash) {
            left = middle + 1;
        } else {
            right = middle;
        }
    }

    if (right == end) {
        right = begin;
    }
    
    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0, "upstream_consistent: continuum pointer %ui", right->value);

    peer = &ucpd->peers[right->index];

    pc->sockaddr = peer->server->addrs->sockaddr;
    pc->socklen = peer->server->addrs->socklen;
    pc->name = &peer->server->addrs->name;

    return NGX_OK;
}

static void
ngx_http_upstream_free_consistent_peer(ngx_peer_connection_t *pc, void *data,
    ngx_uint_t state)
{
    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, pc->log, 0, "upstream_consistent: free peer");

    pc->tries = 0;

    return;
}

static char *
ngx_http_upstream_consistent(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_upstream_srv_conf_t  *uscf;
    ngx_http_upstream_consistent_data_t *ucd;

    uscf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_upstream_module);

    uscf->peer.init_upstream = ngx_http_upstream_init_consistent;

    uscf->flags = (NGX_HTTP_UPSTREAM_CREATE
                   | NGX_HTTP_UPSTREAM_WEIGHT);

    ucd = ngx_pcalloc(cf->pool, sizeof(ngx_http_upstream_consistent_data_t));

    if (ucd == NULL) {
        return NGX_CONF_ERROR;
    }

    uscf->peer.data = ucd;

    return NGX_CONF_OK;
}

static int
ngx_http_upstream_consistent_continuum_item_cmp(const void *t1, const void *t2) {
    ngx_http_upstream_consistent_continuum_item_t *ct1 = (ngx_http_upstream_consistent_continuum_item_t *) t1;
    ngx_http_upstream_consistent_continuum_item_t *ct2 = (ngx_http_upstream_consistent_continuum_item_t *) t2;

    if (ct1->value == ct2->value)
        return 0;
    else if (ct1->value > ct2->value)
        return 1;
    else
        return -1;
}

static u_char*
ngx_http_upstream_consistent_md5_hash(u_char *result, const u_char *key, size_t key_length) {
    ngx_md5_t md5;

    ngx_md5_init(&md5);
    ngx_md5_update(&md5, key, key_length);
    ngx_md5_final(result, &md5);

    return result;
}

static ngx_uint_t
ngx_http_upstream_consistent_ketama_hash(const u_char *key, size_t key_length, ngx_uint_t alignment) {
    u_char results[16];
    
    ngx_http_upstream_consistent_md5_hash(results, key, key_length); 
    
    return ((ngx_uint_t) (results[3 + alignment * 4] & 0xFF) << 24)
         | ((ngx_uint_t) (results[2 + alignment * 4] & 0xFF) << 16)
         | ((ngx_uint_t) (results[1 + alignment * 4] & 0xFF) << 8)
         | (results[0 + alignment * 4] & 0xFF);
}
