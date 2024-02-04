import { LevelUp, LevelUpChain } from 'levelup';
import { HashPath } from './hash_path';
import { Sha256Hasher } from './sha256_hasher';

const MAX_DEPTH = 32;
const LEAF_BYTES = 64; // All leaf values are 64 bytes.

/**
 * The merkle tree, in summary, is a data structure with a number of indexable elements, and the property
 * that it is possible to provide a succinct proof (HashPath) that a given piece of data, exists at a certain index,
 * for a given merkle tree root.
 */
export class MerkleTree {

  private hasher = new Sha256Hasher();
  private root = Buffer.alloc(32);
  /**
   * The data blocks hashed to create the leaf nodes in the Merkle tree, stored sparsely. If index does not exist
   * the value of the data block for that index is assumed to be a 64 byte of zeros.  
   * @type {Map<number, Buffer>}
   */
  private dataBlocks: Map<number, Buffer> = new Map<number, Buffer>(); 
  /**
   * The hash nodes of the Merkle tree stored sparsley. The length is equal to the depth + 1. Each index is the level
   * of the tree. Leaf nodes are at index 0. The root is the only node at index hashes.length - 1.
   * @type {Map<number, Buffer>[]}
   */
  private hashes : Map<number, Buffer>[] = [];
  /**
   * The empty tree default zero hashes for each level of the tree. Length is equal to depth + 1. Each index is the level
   * of the tree. Leaf nodes are at index 0. The root is the only node at index zeroHashesByLevel.length - 1.
   * @type {Buffer[]}
   */
  private zeroHashesByLevel : Buffer[] = [];

  /**
   * Constructs a new MerkleTree instance, either initializing an empty tree, or restoring pre-existing state values.
   * Use the async static `new` function to construct.
   *
   * @param db Underlying leveldb.
   * @param name Name of the tree, to be used when restoring/persisting state.
   * @param depth The depth of the tree, to be no greater than MAX_DEPTH.
   * @param data When restoring, you need to provide the data.
   */
  constructor(private db: LevelUp, private name: string, private depth: number, data?: string) {
    if (!(depth >= 1 && depth <= MAX_DEPTH)) {
      throw Error('Bad depth');
    }
    this.db = db;
    this.name = name;
    this.depth = depth;
    this.generateZeroHashesByLevel();
    if (data) {
      this.restorePersistedTree(data);
    } else {
      this.generateTreeNodes();
    }
  }

  /**
   * Generates the default empty string hashes for each level of the tree.
   */
  generateZeroHashesByLevel () {
    this.zeroHashesByLevel[0] = this.hasher.hash(Buffer.alloc(LEAF_BYTES))
    for (let i = 1; i < this.depth + 1; i++) { // start at index 1 because leaf level hash already added at index 0
      this.zeroHashesByLevel[i] = this.hasher.compress(this.zeroHashesByLevel[i - 1], this.zeroHashesByLevel[i - 1]);
    }
    this.root = this.zeroHashesByLevel[this.zeroHashesByLevel.length - 1]
  }

  /**
   * The index of the parent node, one level above, for a given index.
   * @param index 
   * @returns index of the parent node
   */
  indexOfParentNode (index: number) : number {
    if (index % 2 === 0) {
      return index/2;
    } else {
      return Math.round(index/2) - 1;
    }
  }

  /**
   * Generates tree nodes (hashes) for a new tree and when data blocks are updated.
   */
  generateTreeNodes() {
    this.hashes = [new Map<number, Buffer>()]
    for (const [index, buffer] of this.dataBlocks.entries()) {
      this.hashes[0].set(index, this.hasher.hash(buffer))
    }
    
    // Calculate hash at each level above
    for (let i = 1; i < this.depth + 1; i++) { // start at index 1 because leaf level hash already calculated
        this.hashes.push(new Map<number, Buffer>())
        const hashesLevelBefore = this.hashes[i-1];
        const processed = new Map<number, boolean>(); // Using memory to keep track of visited. Could alternatively sort by index and skip entries, but assuming very sparse.
        for (const [index, hash] of hashesLevelBefore.entries()) {
          const indexOfSibling = index % 2 === 0 ? index + 1 : index - 1;
          if (!(processed.has(indexOfSibling) && processed.has(index))) {
            const hashOfSiblingNode : Buffer = hashesLevelBefore.has(indexOfSibling) ? hashesLevelBefore.get(indexOfSibling)! : this.zeroHashesByLevel[i-1]
            this.hashes[i].set(this.indexOfParentNode(index), this.hasher.compress(hash, hashOfSiblingNode));
          } 
          processed.set(index, true);
          processed.set(indexOfSibling, true);
        }
    }
    this.root = this.hashes[this.hashes.length - 1].size === 0 ? this.zeroHashesByLevel[this.zeroHashesByLevel.length - 1] : this.hashes[this.hashes.length - 1].get(0)! ;
  }

  /**
   * Constructs or restores a new MerkleTree instance with the given `name` and `depth`.
   * The `db` contains the tree data.
   */
  static async new(db: LevelUp, name: string, depth = MAX_DEPTH) {
    const data: string = await db.get(name).catch(() => {});
    const tree = new MerkleTree(db, name, depth, data);
    await tree.persistTree();
    return tree;
  }

  /**
   * Persists tree (just the nodes/hashes, not the data blocks) to the database.
   * This is being done here by JSON stringifying the tree. There are alternate
   * methods of packing this more tightly using buffer arrays. In the case we
   * want to elect to only store the leaf hashes, and recalcualte other hashes
   * on restore the implementation for that is relatively straightforward and 
   * would save space. Open to discussion.
   */
  private async persistTree () {
    // Storing hashes only
    const data: any = {}
    for (let i = 0; i < this.hashes.length; i++) {
      const levelHashes: Map<number, Buffer> = this.hashes[i];
      data[i] = {};
      for (const [index, value] of levelHashes.entries()) {
        data[i][index] = value.toJSON();
      }
    }
    await this.db.put(this.name, JSON.stringify(data));
  }

  /**
   * Restores tree perisisted to database to memory.
   * @param data 
   */
  private restorePersistedTree (data: string) {
    if (data) {
      const persistedData = JSON.parse(data);
      this.hashes = [];
      this.depth = 0;
      Object.keys(persistedData).forEach((key) => {
        this.depth += 1
        this.hashes[parseInt(key, 10)] = new Map<number, Buffer>();
        const persistedDataForDepth = persistedData[key];
        Object.keys(persistedDataForDepth).forEach((indexString) => {
          this.hashes[parseInt(key, 10)].set(parseInt(indexString, 10), Buffer.from(persistedDataForDepth[indexString]));
        })
      })
      this.depth -= 1;
      this.root = this.hashes[this.hashes.length - 1].get(0)!;
    }
  }

  getRoot() {
    return this.root;
  }

  /**
   * Returns the hash path for `index`.
   * e.g. To return the HashPath for index 2, return the nodes marked `*` at each layer.
   *     d0:                                            [ root ]
   *     d1:                      [*]                                               [*]
   *     d2:         [*]                      [*]                       [ ]                     [ ]
   *     d3:   [ ]         [ ]          [*]         [*]           [ ]         [ ]          [ ]        [ ]
   */
  async getHashPath(index: number) {
    const hashPath = new HashPath();
    for (let i = 0; i < this.depth; i++) {
      const path : Buffer[] = [];
      const indexOfSibling = index % 2 === 0 ? index + 1 : index - 1;
      if (this.hashes[i].has(index)) {
        path[index < indexOfSibling ? 0 : 1] = this.hashes[i].get(index)!;
      } else {
        path[index < indexOfSibling ? 0 : 1] = this.zeroHashesByLevel[i];
      }
      if (this.hashes[i].has(indexOfSibling)) {
        path[indexOfSibling < index ? 0 : 1] = this.hashes[i].get(indexOfSibling)!;
      } else {
        path[indexOfSibling < index ? 0 : 1] = this.zeroHashesByLevel[i];
      }
      hashPath.data.push(path);
      index = this.indexOfParentNode(index);
    }
    return hashPath;
  }

  /**
   * Updates the tree with `value` at `index`. Returns the new tree root.
   */
  async updateElement(index: number, value: Buffer) {
    this.dataBlocks.set(index, value);
    this.generateTreeNodes();
    await this.persistTree();
    return this.root;
  }
}
