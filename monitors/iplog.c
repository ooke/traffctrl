#include <stdio.h>
#include <stdlib.h>
#include <pcap/pcap.h>
#include <netinet/if_ether.h>
#include <arpa/inet.h>
#include <netinet/ip.h>
#include <netinet/ip6.h>
#include <time.h>
#include <stdio.h>
#include "uthash.h"
#include "utlist.h"

struct data_t {
    char ipaddr[128];
    unsigned long long int out_bytes, in_bytes, pkts;
    UT_hash_handle hh;
};

struct ip_t {
    char ipaddr[128];
    struct ip_t *next, *prev;
};

static struct timespec last_cp;
static char *out_fname = NULL;
static char *tmp_fname = NULL;
static unsigned int write_timeout = 0;
static struct data_t *data_table = NULL;
struct ip_t *local_ips = NULL;
struct ip_t *local_nets = NULL;

static inline int startswith(char *str, char *start) {
#ifdef DEBUG
    printf("startswith('%s', '%s') -> ", str, start);
#endif
    if (*str != '\0' && *start != '\0')
        while (*str == *start) {
            ++str;
            ++start;
            if (*start == '\0') {
#ifdef DEBUG
                printf("1\n");
#endif
                return 1;
            }
        }
#ifdef DEBUG
    printf("0\n");
#endif
    return 0;
}

static inline int not_in_ips(char *ip) {
    struct ip_t* my_ip = NULL;
#ifdef DEBUG
    printf("not_in_ips('%s') -> ", ip);
#endif
    DL_FOREACH (local_ips, my_ip) {
        if (strcmp (my_ip->ipaddr, ip) == 0) {
#ifdef DEBUG
            printf("0\n");
#endif
            return 0;
        }
    };
#ifdef DEBUG
    printf("1\n");
#endif
    return 1;
}

void callback(u_char *useless, const struct pcap_pkthdr* pkthdr, const u_char* packet) {
  struct ether_header *eth_header;
  unsigned int ip_length = 0;
  char ip_src[128], ip_dst[128], *the_ip = NULL;
  unsigned long long int out_bytes = 0, in_bytes = 0;
  struct data_t *data = NULL, *tmp = NULL;
  struct ip_t *net = NULL;
  struct timespec curr_cp;

  ip_src[0] = ip_dst[0] = '\0';
  ip_length = pkthdr->len;
  eth_header = (struct ether_header *) packet;
  if (ntohs(eth_header->ether_type) == ETHERTYPE_IP) {
      const struct ip *ip_header = (struct ip *)&(packet[ETHER_HDR_LEN]);
      inet_ntop(AF_INET, (void*)&(ip_header->ip_src), ip_src, sizeof(ip_src));
      inet_ntop(AF_INET, (void*)&(ip_header->ip_dst), ip_dst, sizeof(ip_dst));
  } else if (ntohs(eth_header->ether_type) == ETHERTYPE_IPV6) {
      const struct ip6_hdr *ip_header = (struct ip6_hdr *)&(packet[ETHER_HDR_LEN]);
      inet_ntop(AF_INET6, (void*)&(ip_header->ip6_src), ip_src, sizeof(ip_src));
      inet_ntop(AF_INET6, (void*)&(ip_header->ip6_dst), ip_dst, sizeof(ip_dst));
  } else return;

  if (ip_src[0] == '\0' || ip_dst[0] == '\0') return;

  DL_FOREACH (local_nets, net) {
      if (startswith (ip_src, net->ipaddr) && not_in_ips (ip_src)) {
          the_ip = ip_src;
	  //other_ip = ip_dst;
          out_bytes = ip_length;
          break;
      } else if (startswith (ip_dst, net->ipaddr) && not_in_ips (ip_dst)) {
          the_ip = ip_dst;
	  //other_ip = ip_src;
          in_bytes = ip_length;
          break;
      }
  }

  if (the_ip == NULL) return;

  HASH_FIND_STR (data_table, the_ip, data);
  if (data == NULL) {
      data = (struct data_t *) malloc (sizeof (struct data_t));
      if (data == NULL) {
          fprintf(stderr, "ERROR: failed to allocate memory.\n");
          exit(20);
      }
      strcpy (data->ipaddr, the_ip);
      data->out_bytes = 0;
      data->in_bytes = 0;
      data->pkts = 0;
      HASH_ADD_STR (data_table, ipaddr, data);
  }
  data->out_bytes += out_bytes;
  data->in_bytes += in_bytes;
  data->pkts += 1;

  if (clock_gettime(CLOCK_MONOTONIC, &curr_cp) < 0) {
      fprintf(stderr, "ERROR: could not get monotonic time.\n");
      exit(19);
  }
  if (curr_cp.tv_sec - last_cp.tv_sec > write_timeout) {
      FILE *fd = fopen(tmp_fname, "w");
      if (fd == NULL) {
          fprintf(stderr, "ERROR: file %s could not be opened.\n", tmp_fname);
          exit(18);
      }
      last_cp.tv_sec = curr_cp.tv_sec;
      HASH_ITER (hh, data_table, data, tmp) {
          fprintf(fd, "%s %lld %lld %lld\n", data->ipaddr, data->out_bytes, data->in_bytes, data->pkts);
      };
      fclose(fd);
      if (rename (tmp_fname, out_fname) != 0) {
          fprintf(stderr, "ERROR: failed to move file %s to file %s.\n", tmp_fname, out_fname);
          exit(18);
      }
  }
}

int main(int argc, char **argv) {
    if (argc != 6) goto usage;

    char *iface = argv[1];
    char *my_ips = strdup(argv[4]);
    char *my_nets = strdup(argv[5]);
    char errbuf[PCAP_ERRBUF_SIZE];
    out_fname = argv[2];
    tmp_fname = (char*) malloc (strlen(out_fname) + 4);
    sprintf (tmp_fname, "%s.tmp", out_fname);
    write_timeout = atoi(argv[3]);

    if (write_timeout < 5 || write_timeout > 216000) {
        fprintf(stderr, "ERROR: write timeout can be between 5 and 216000\n");
        goto usage;
    }

    for (int i = 0; my_ips[i] != '\0'; i++) {
        int finished = (my_ips[i] == '\0');
        if (my_ips[i] == ' ' || finished) {
            struct ip_t *el = (struct ip_t *) malloc (sizeof (struct ip_t));
            if (el == NULL) {
                fprintf(stderr, "ERROR: failed to allocate memory.\n");
                exit(20);
            }
            my_ips[i] = '\0';
            strcpy (el->ipaddr, my_ips);
            DL_APPEND (local_ips, el);
            if (finished) break;
            my_ips = &(my_ips[i + 1]);
            i = 0;
        }
    }

    for (int i = 0;; i++) {
        int finished = (my_nets[i] == '\0');
        if (my_nets[i] == ' ' || finished) {
            struct ip_t *el = (struct ip_t *) malloc (sizeof (struct ip_t));
            if (el == NULL) {
                fprintf(stderr, "ERROR: failed to allocate memory.\n");
                exit(20);
            }
            my_nets[i] = '\0';
            strcpy (el->ipaddr, my_nets);
            DL_APPEND (local_nets, el);
            if (finished) break;
            my_nets = &(my_nets[i + 1]);
            i = 0;
        }
    }

    int snaplen = ETHER_HDR_LEN + (sizeof (struct ip) > sizeof (struct ip6_hdr) ? sizeof (struct ip) : sizeof (struct ip6_hdr));
    pcap_t *descr = pcap_open_live(iface, snaplen, 0, 1000, errbuf);
    if (descr == NULL) {
        fprintf(stderr, "ERROR: pcap_open_live(%s) failed: %s\n", iface, errbuf);
        return 1;
    }

    if (clock_gettime(CLOCK_MONOTONIC, &last_cp) < 0) {
        fprintf(stderr, "ERROR: could not get monotonic time.\n");
        return 19;
    }
    if (pcap_loop(descr, 0, callback, NULL) == -1) {
        fprintf(stderr, "ERROR: pcap_loop() failed.\n");
        return 1;
    }

    return 0;

usage:
    fprintf(stderr, "Usage: %s <interface> <output file> <write timeout> <my ips> <my nets>\n", argv[0]);
    return 1;
}
