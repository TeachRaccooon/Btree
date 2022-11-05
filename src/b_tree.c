#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "jdisk.h"
#include "../include/b_tree.h"

/*
Write a btree info onto the disk that it is associated with.
*/
void write_tree(B_Tree *btree)
{
   unsigned char buf[1024];
   // The first 4 bytes are for the key size
   *((unsigned int *)(buf)) = btree->key_size;
   // the next 4 bytes define the root lba
   *((unsigned int *)(buf + 4)) = btree->root_lba;
   // The first free lba on the disk
   *((unsigned long int *)(buf + 8)) = btree->first_free_block;

   // Write  the buffer to the disk
   jdisk_write(btree->disk, 0, (void*)buf);
}

/*
Reads the btree info from the disk.
*/
void read_tree(B_Tree *btree)
{
   unsigned char buf[1024];
   jdisk_read(btree->disk, 0, (void*)buf);

   // Basically an inverse operation of the above
   btree->key_size = *(unsigned int*)(buf);
   btree->root_lba = *(unsigned int*)(buf + 4);
   btree->first_free_block = *(unsigned long int*)(buf + 8);
   // allocate space for the root
   btree->root = malloc(sizeof(Tree_Node));

   // num sectors
   btree->num_lbas = btree->size / JDISK_SECTOR_SIZE;
   // Maxkey
   btree->keys_per_block =  (1024 - 6) / (btree->key_size + 4);

   // Also read in the root node
   read_node(btree, btree->root, btree->root_lba, NULL);
}

void write_node(B_Tree *btree, Tree_Node *node)
{
   if(btree->keys_per_block < node->nkeys)
   {
      // Check with Plank's msg
      fprintf(stderr, "Node exceeds MAXKEY.\n");
   }

   unsigned char buf[1024];
   // First byte signifying whether the node is internal
   memcpy(buf,   &(node->internal), 1);
   // 2nd byte signifying the number of keys in the node
   memcpy(buf+1, &(node->nkeys), 1);

   int k_sz = btree->key_size;
   // copying all the keys over to the buffer - but can i just copy all the bytes at once?
   for(int i = 0; i < (int) (node->nkeys); ++i)
   {
      // Don't forget the offset by 2!
      memcpy(buf + 2 + (i * k_sz), node->keys[i], k_sz);
   }

   // How many bytes do LBA's occupy in a node (remember about an additional one)
   unsigned int lba_space_sz = ((int) (btree->keys_per_block) + 1) * sizeof(unsigned int);
   // Copy over all aba's
   memcpy(buf + 1024 - lba_space_sz, node->lbas, lba_space_sz);

   // Write the buffer into the disk
   jdisk_write(btree->disk, node->lba, (void*)buf);
}


// Should i pass the parent in here?
void read_node(B_Tree *btree, Tree_Node *node, unsigned int lba, Tree_Node* parent)
{

   if(node == NULL)
   {
      fprintf(stderr, "Error: Please pass a pre-allocated node.\n");
      exit(1);
   }

   unsigned char buf[1024];
   jdisk_read(btree->disk, 0, (void*) buf);

   // Pretty much doing an inverse of the above function, filling an empty node with data
   node->internal = buf[0];
   node->nkeys    = buf[1];
   // Allocate space for keys
   node->keys     = malloc((btree->keys_per_block + 1) * sizeof(char *));

   for(int i = 0; i < btree->keys_per_block + 1; ++i)
   {
      // Allocate space for individual keys
      node->keys[i]  = malloc(btree->key_size);
   }

   node->lbas = malloc((btree->keys_per_block + 2) * sizeof(unsigned int));
   node->children  = malloc((btree->keys_per_block + 2) * sizeof(Tree_Node*));
   node->lba  = lba;

   int k_sz = btree->key_size;

   // Copying the keys over - but can i just copy all the bytes at once?
   for(int i = 0; i < (int) (node->nkeys); ++i)
   {
      memcpy(node->keys[i], buf + 2 + i * k_sz, k_sz);
   }

   // Copy existing lbas into the node
   memcpy(node->lbas, buf + 1024 - (btree->keys_per_block + 1) * sizeof(unsigned int), ((int) (node->nkeys) + 1) * sizeof(unsigned int));

   node->parent = parent;
}

/*
This will just create sector 1, a root node sector.
*/
void *b_tree_create(char *filename, long size, int key_size)
{
   if(key_size <= 0)
   {
      return NULL;
   }

   // Allocate a tree
   B_Tree *mytree = malloc(sizeof(B_Tree));

   void* mydisk = jdisk_create(filename, size);

   // Fill the elements of a tree structure

   mytree->key_size = key_size;
   mytree->root_lba = 1;
   // Root is not the first free node
   mytree->first_free_block = 2;

   mytree->disk = mydisk;     
   mytree->size = size;      
   mytree->num_lbas = mytree->size / JDISK_SECTOR_SIZE;
   // Maxkey
   mytree->keys_per_block = (1024 - 6) / (key_size + 4);
   // Maxkey + 1
   mytree->lbas_per_block = mytree->keys_per_block + 1;

   /* When find() fails, this is a pointer to the external node */
   mytree->tmp_e = NULL;             
   //mytree->tmp_e_index;              /* and the index where the key should have gone */ - leave empty for now?
   mytree->flush = 0;

   // We now need to create a root node
   Tree_Node *root = malloc(sizeof(Tree_Node));
   root->nkeys = 0;
   root->flush = 0;
   root->internal = 0;
   root->lba = 1;
   // just allocate some space for these two
   root->keys = calloc((mytree->keys_per_block + 1), sizeof(char*));
   for(int i = 0; i < mytree->keys_per_block + 1; ++i)
   {
      root->keys[i] = calloc(1, mytree->key_size);
   }
   root->lbas = calloc((mytree->keys_per_block + 2), sizeof(unsigned int));
   root->children = malloc((mytree->keys_per_block + 2) * sizeof(Tree_Node*));
   for(int i = 0; i < mytree->keys_per_block + 2; ++i)
   {
      root->children[i] = malloc(sizeof(Tree_Node));
   }
   root->parent = NULL;
   // What should i do here?
   //root->ptr;                        /* Free list link */

   mytree->root = root;
   //mytree->free_list = root;         /* Free list of nodes - i'm guessing that this should point to the root? */

   // Actually write stuff on a disk
   write_tree(mytree);
   write_node(mytree, root);

   return (void *) mytree;
}

void *b_tree_attach(char *filename)
{
   B_Tree *mytree = malloc(sizeof(B_Tree));
   // Attach some file to an empty disk, associated with a newly-created tree
   mytree->disk = jdisk_attach(filename);

   // Read that btree
   read_tree(mytree);

   return (void *)mytree;
}


/*
Finding a value associated with a key.

Returns LBA of the val associated with the key.
If key is not in the tree, returns 0.
*/
unsigned int b_tree_find(void *b_tree, void *key)
{
   B_Tree* mytree = ((B_Tree *)b_tree);

   // The node that we keep track of at any iteration
   Tree_Node *curr_node = mytree->root;

   // Indicator stating whether the key has been identified
   int found_key = 0;

   printf("In Find\n");
   // Iterate while we're on internal node. Otherswise, return 0
   while(1)
   {
      if(curr_node->nkeys == 0 && found_key == 0)
      {
         // Likely an empty root type situation, nothing was found too
         mytree->tmp_e = curr_node;
         return 0;
      }

      if(found_key)
      {

         // if we're at an external node, grab the val and return
         if(!(curr_node->internal))
         {
            return curr_node->lbas[(int)(curr_node->nkeys)];
         }

         // Now we just want to get to the external node asap
         // So grab the rightmost child
         // Need to actually read the child node from the disk
         //printf("Key found\n");
         //printf("nkeys in the node %d\n", (int)(curr_node->nkeys));
         // Need to make sure a child node exists
         //if(!curr_node->children[(int)(curr_node->nkeys)])
         //{
            curr_node->children[(int)(curr_node->nkeys)] = malloc(sizeof(Tree_Node));
            read_node(mytree,  curr_node->children[(int)(curr_node->nkeys)], curr_node->lbas[(int)(curr_node->nkeys)], curr_node);
         //}

         curr_node = curr_node->children[ (int)(curr_node->nkeys)];
      }
      else
      {
         // Iterate through the keys in the node
         for(int i = 0; i < (int)(curr_node->nkeys); ++i)
         {
            int compare = memcmp(key, curr_node->keys[i], mytree->key_size);

            // Check if the keys are matching
            if(!compare)
            {
               // We've found the key, now we need to find the value (in another node)
               // In the convention we're using, it will be in the right pointer of the
               // smaller node.
               // So we're jumping to the left child and then skipping the key comparison in
               // any further alg iterations, grabbing the rightmost child right away
               found_key = 1;

               // If we're at an external node, then we're done lol
               if(!(curr_node->internal))
               {
                  return curr_node->lbas[i];
               }
               printf("Key found now\n");
               printf("nkeys in the node %d\n", (int)(curr_node->nkeys));

               // Need to make sure a child node exists
               //if(!curr_node->children[i])
               //{
                  curr_node->children[i] = malloc(sizeof(Tree_Node));
                  // Need to actually read the child node from the disk
                  read_node(mytree,  curr_node->children[i], curr_node->lbas[i], curr_node);
               //}
               curr_node = curr_node->children[i];
               break;
            }
            else if(compare < 0)
            {
               // In this case, the the key is in a "smaller" node
               // We need to look at the left child of the key

               // If we're at an external node, no key will be found - terminate
               if(!(curr_node->internal))
               {
                  // pointer to external node
                  mytree->tmp_e = curr_node;
                  return 0;
               }
               printf("Key less\n");
               // Need to make sure a child node exists
               //if(!curr_node->children[i])
               //{
                  curr_node->children[i] = malloc(sizeof(Tree_Node));
                  // Need to actually read the child node from the disk
                  read_node(mytree,  curr_node->children[i], curr_node->lbas[i], curr_node);
               //}
               curr_node = curr_node->children[i];
               break;
            }
            else if(compare > 0 && i == mytree->keys_per_block)
            {
               // the default situation here would be to just go to the next key
               // But if we're at the last key, jump to the rightmost child 

               // If we're at an external node, no key will be found - terminate
               if(!(curr_node->internal))
               {
                  // pointer to external node
                  mytree->tmp_e = curr_node;
                  return 0;
               }
               printf("Key greater jump\n");
               // Need to make sure a child node exists
               //if(!curr_node->children[i + 1])
               //{
                  curr_node->children[i + 1] = malloc(sizeof(Tree_Node));
                  // Need to actually read the child node from the disk
                  read_node(mytree,  curr_node->children[i + 1], curr_node->lbas[i + 1], curr_node);
               //}
               curr_node = curr_node->children[i + 1];
               break;
            }
         }
      }
   }

   // Means we've somehow failed
   return -1;
}

void shift_node_dat(Tree_Node *node, int i)
{
   int j = (int) (node->nkeys);
   // Iterate from the end of all lists
   // remember that there's an additional lba and child
   node->children[j+1] = node->children[j];
   node->lbas[j+1] = node->lbas[j];
   j -= 1;

   for(; j >= i; --j)
   {
      node->keys[j + 1] = node->keys[j];
      node->lbas[j + 1] = node->lbas[j];
      node->children[j + 1] = node->children[j];
   }
}

unsigned int b_tree_insert(void *b_tree, void *key, void *record)
{
   printf("In Insert\n");
 
   int lba = b_tree_find(b_tree, key);

   B_Tree* mytree = (B_Tree*) b_tree;
   b_tree_print_tree(mytree);

   if(lba) 
   {
      // key found, p, place record into val
      jdisk_write(((B_Tree*) b_tree)->disk, lba, record);

      // Do we need to update the btree itself now?
      return lba;
   }
   
   else
   {
      // We need to find an appropriate place for the record to be inserted
      // suppose we've found the external node where this key belongs 
      Tree_Node *node_found = mytree->root;

      // Search for a place in the found node to insert the key
      int i = 0;
      while(memcmp(key, node_found->keys[i], mytree->key_size) > 0 && (int) *(node_found->keys[i]) != 0)
      {
         ++i;
      }
      // shift all keys to the right by one 
      // in the same loop, shift all the lbas and children

      shift_node_dat(node_found, i);

      // lba of the val
      unsigned long val_lba = mytree->first_free_block;

      // modify the tree
      mytree->first_free_block = mytree->first_free_block + 1;

      // place the new data at i
      node_found->keys[i] = key;
      node_found->lbas[i] = val_lba;
      node_found->children[i] = record;

      node_found->nkeys = (unsigned char) ((int) (node_found ->nkeys) + 1);

      // check if we've exceeded maxkey
      if((int)(node_found->nkeys) > mytree->keys_per_block)
      {
         printf("Splitting node\n");
         // oh boy here we fucking go - need to split
         
         // grab a midpoint of keys
         // this will be the key that we will be moving up
         int midkey = (int)(node_found->nkeys) / 2;

         // make an empty node
         // everything from the right to it gets copied to a new node
         Tree_Node *newnode = malloc(sizeof(Tree_Node));
         
         newnode->keys      = malloc((mytree->keys_per_block + 1) * sizeof(char*));
         newnode->lbas      = malloc((mytree->keys_per_block + 2) * sizeof(unsigned int));
         newnode->children  = malloc((mytree->keys_per_block + 2) * sizeof(Tree_Node*));
         // now, make copies
         // copying keys
         int i = midkey + 1, j = 0;
         for(; i < (int) (node_found->nkeys); ++i, ++j)
         {
            memcpy(node_found->keys[i], newnode->keys[j], sizeof(char*));
            newnode->lbas[j] = node_found->lbas[i];
            memcpy(node_found->children[i], newnode->children[j], sizeof(Tree_Node*));

            // we also need to update the old node here
            // is this correct?
            node_found->keys[i] = 0;
            node_found->lbas[i] = 0;
            node_found->children[i] = NULL;
         }
         // one additional child and LBA
         newnode->lbas[j] = node_found->lbas[i];
         memcpy(node_found->children[i], newnode->children[j], sizeof(Tree_Node*));

         newnode->nkeys = j;
         //newnode->flush = 0;
         newnode->internal = 0;
         newnode->lba = mytree->first_free_block;

         // previous node exists
         if(node_found->parent != NULL)
         {
            // find where the midkey key belongs
            int i = 0;
            while(memcmp(node_found->keys[midkey], node_found->parent->keys[i], mytree->key_size) > 0 && node_found->parent->keys[i] != 0)
            {
               ++i;
            }
            // shift everything to the right
            shift_node_dat(node_found->parent, i);

            // place the new data at i
            node_found->parent->keys[i] = node_found->keys[midkey];
            // the shift here works a bit weird
            node_found->parent->lbas[i] = node_found->lba;
            node_found->parent->lbas[i + 1] = newnode->lba;

            node_found->parent->children[i] = node_found;
            node_found->parent->children[i + 1] = newnode;

            newnode->parent = node_found->parent;
            node_found->parent->nkeys = (char) (((int) node_found->parent->nkeys) + 1);
         }
         else
         {
            //We need to create a parent node in this case
            // this will be the new root node then, so brace urself lol
            node_found->parent = malloc(sizeof(Tree_Node));
         
            node_found->parent->keys      = malloc((mytree->keys_per_block + 1) * sizeof(char*));
            node_found->parent->lbas      = malloc((mytree->keys_per_block + 2) * sizeof(unsigned int));
            node_found->parent->children  = malloc((mytree->keys_per_block + 2) * sizeof(Tree_Node*));

            node_found->parent->parent = NULL;
            node_found->parent->nkeys = 1;
            node_found->parent->internal = 1;

            // place the new data at i
            node_found->parent->keys[0] = node_found->keys[midkey];
            // the shift here works a bit weird
            node_found->parent->lbas[0] = node_found->lba;
            node_found->parent->lbas[1] = newnode->lba;

            node_found->parent->children[0] = node_found;
            node_found->parent->children[1] = newnode;

            newnode->parent = node_found->parent;

            // need to update the btree now
            mytree->root = node_found->parent;

            // Update the first free node
            mytree->first_free_block = mytree->first_free_block + 1;
         }

         // update the number of keys in the old node
         node_found->nkeys = (char)(midkey - 1);

         // Update the first free node
         mytree->first_free_block = mytree->first_free_block + 1;

         // Now, write the node_found->parent and newnode
         write_node(mytree, node_found->parent);
         write_node(mytree, newnode);
      }
      
      // write node_found and btree
      write_node(mytree, node_found);
      write_tree(mytree);
      // write data
      jdisk_write(mytree->disk, node_found->lbas[i], record);

      return val_lba;
   }
   return -1;
}

// Just use the convenient btree struct 
void *b_tree_disk(void *b_tree) 
{
    return ((B_Tree *)b_tree) -> disk;
}

// Same as prev function
int b_tree_key_size(void *b_tree) 
{
    return ((B_Tree *)b_tree) -> key_size;
}

/*
Auxillary printing routines
*/

unsigned int get_node_level(Tree_Node *node)
{
   int level;
   for(level = 0; node->parent != NULL; level++)
   {
      node=node->parent;
   }
   return level;
}

void print_node(B_Tree *tree, Tree_Node *node)
{
   printf("block at lba %u (level %u)\n", node->lba, get_node_level(node));
   printf("num keys: %d\n", (int) (node->nkeys));
   for(int i = 0; i < (int) (node->nkeys); ++i)
   {
      printf("   key %d: %s\n", i, node->keys[i]);
   }
   for(int i = 0; i < (int) (node->nkeys) + 1; ++i)
   {
      printf("   lba %d: %u\n", i, node->lbas[i]);
   }
   if(node->internal)
   {
      for(int i = 0; i <(int) (node->nkeys) + 1; ++i)
      {
         /* read in the node to cache if we haven't done so yet. */
         if(!(node->children[i]))
         {
            read_node(tree, node->children[i], node->lbas[i], node);
         }
         print_node(tree, node->children[i]);
      }
   }
   return;
}

void b_tree_print_tree(B_Tree *tree)
{
   printf("b_tree information\n");
   printf("key size: %u\n", tree->key_size);
   printf("root lba: %u\n", tree->root_lba);
   printf("sectors:  %lu\n", tree->first_free_block);
   printf("\n");
   printf("on a jdisk with %lu sectors\n", tree->num_lbas);
   printf("with %d keys per node\n", tree->keys_per_block);

   /* now load in the root node, if not already loaded */
   if(!(tree->root))
   {
      read_node(tree, tree->root, tree->root_lba, NULL);
   }

   print_node(tree, tree->root);

   return;
}

