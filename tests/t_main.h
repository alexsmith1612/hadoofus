#ifndef _T_MAIN_H
#define _T_MAIN_H

#ifndef nelem
# define nelem(ARR) (sizeof(ARR) / sizeof(ARR[0]))
#endif

#define HDFS_T_ADDR "HDFS_TEST_NODE_ADDRESS"
#define HDFS_T_PORT "HDFS_TEST_NODE_PORT"
#define HDFS_T_USER "HDFS_TEST_USER"
#define HDFS_T_VER "HDFS_TEST_VER"
#define HDFS_T_KERB "HDFS_TEST_KERB"

Suite *		t_hl_rpc_basics_suite(void);
Suite *		t_datanode_basics_suite(void);
Suite *		t_unit(void);
Suite *		t_namenode_nb_suite(void);
Suite *		t_datanode_nb_suite(void);

extern const char *H_ADDR, *H_USER, *H_PORT;
extern enum hdfs_namenode_proto H_VER;
extern enum hdfs_kerb H_KERB;

extern const char *const csum2str[];

#endif
