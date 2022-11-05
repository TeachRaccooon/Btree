#ifndef _BP_TREE_
#define _BP_TREE_

#include "jdisk.h"

typedef struct tnode {
   unsigned char bytes[1024+256]; /* This holds the sector for reading and writing.  
                                                   It has extra room because your internal representation  
                                                   will hold an extra key. */
   unsigned char nkeys;                      /* Number of keys in the node */
   unsigned char flush;                      /* Should I flush this to disk at the end of b_tree_insert()? */
   unsigned char internal;                   /* Internal or external node */
   unsigned int lba;                         /* LBA when the node is flushed */
   unsigned char **keys;                     /* Pointers to the keys.  Size = MAXKEY+1 */
   unsigned int *lbas;                       /* Pointer to the array of LBA's.  Size = MAXKEY+2 */
   struct tnode *parent;                     /* Pointer to my parent -- useful for splitting */
   struct tnode **children;                  /* Multiple nodes -- will simplify handling shit greatly*/
   int parent_index;                         /* My index in my parent */
   struct tnode *ptr;                        /* Free list link */
} Tree_Node;

typedef struct {
   int key_size;                 /* These are the first 16/12 bytes in sector 0 */
   unsigned int root_lba;
   unsigned long first_free_block;

   void *disk;                   /* The jdisk */
   unsigned long size;           /* The jdisk's size */
   unsigned long num_lbas;       /* size/JDISK_SECTOR_SIZE */
   int keys_per_block;           /* MAXKEY */
   int lbas_per_block;           /* MAXKEY+1 */
   Tree_Node *free_list;         /* Free list of nodes */
   Tree_Node *root;              /* Root node*/

   Tree_Node *tmp_e;             /* When find() fails, this is a pointer to the external node */
   int tmp_e_index;              /* and the index where the key should have gone */

   int flush;                    /* Should I flush sector[0] to disk after b_tree_insert() */
} B_Tree;

void write_tree(B_Tree *btree);
void read_tree(B_Tree *btree);

void write_node(B_Tree *btree, Tree_Node *node);
void read_node(B_Tree *btree, Tree_Node *node, unsigned int lba, Tree_Node* parent);

void shift_node_dat(Tree_Node *node, int i);

unsigned int get_node_level(Tree_Node *node);
void print_node(B_Tree *tree, Tree_Node *node);
void b_tree_print_tree(B_Tree *tree);

void *b_tree_create(char *filename, long size, int key_size);
void *b_tree_attach(char *filename);

unsigned int b_tree_insert(void *b_tree, void *key, void *record);
unsigned int b_tree_find(void *b_tree, void *key);
void *b_tree_disk(void *b_tree);
int b_tree_key_size(void *b_tree);

#endif
