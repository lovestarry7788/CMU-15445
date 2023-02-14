#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // 查找 key 对应的 leaf_page
  auto leaf_page = FindLeaf(key, Operation::Find, transaction);
  auto leaf_node = reinterpret_cast<LeafPage *>(leaf_page -> GetData());

  ValueType value;
  bool is_exist = leaf_node -> Lookup(key, &value, comparator_);
  // 用完需要直接 Unpin 掉
  buffer_pool_manager_ -> UnpinPage(leaf_page -> GetPageId(), false);

  if (!is_exist) {
    return false;
  }
  result->emplace_back(value);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, Operation operation, Transaction *transaction, bool leftMost, bool rightMost) -> Page* {
  auto page = buffer_pool_manager_ ->FetchPage(root_page_id_);
  auto node = reinterpret_cast<BPlusTreePage *>(page -> GetData());

  /*
  if (operation == Operation :: Find) {
    page -> RLatch();
  }
  */

  while(!node -> IsLeafPage()) {
    auto *i_node = reinterpret_cast<InternalPage *>(node);
    page_id_t child_node_page_id;
    if(leftMost) {
      child_node_page_id = i_node -> ValueAt(0);
    } else if(rightMost) {
      child_node_page_id = i_node -> ValueAt(i_node -> GetSize() - 1);
    } else {
      child_node_page_id = i_node -> Lookup(key, comparator_);
    }

    auto child_page = buffer_pool_manager_ ->FetchPage(child_node_page_id);
    auto child_node = reinterpret_cast<BPlusTreePage *>(page -> GetData());

    /* 释放原来的页和锁，对孩子节点的页加读锁
    if(operation == Operation::Find) {
      page -> RUnlatch();
      child_page -> RLatch();
      buffer_pool_manager_ -> UnpinPage(page -> GetPageId(), false);
    }
    */

    page = child_page;
    node = child_node;
  }

  return page;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  //{
  //  std::scoped_lock lock(latch_);
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  //}
  return InsertIntoLeaf(key, value, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  // 1. 向缓存池中申请一个 new page, 作为 root page
  auto root_page = buffer_pool_manager_ ->NewPage(&root_page_id_);
  if(root_page == nullptr) {
    throw std::runtime_error("Cannot start new tree!");
  }

  // 2. 更新 header page，添加索引的信息
  UpdateRootPageId(1);

  auto root_node = reinterpret_cast<LeafPage *>(root_page -> GetData());
  root_node -> Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  root_node -> Insert(key ,value, comparator_);

  buffer_pool_manager_ -> UnpinPage(root_page -> GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  auto leaf_page = FindLeaf(key);
  auto leaf_node = reinterpret_cast<LeafPage*>(leaf_page -> GetData());

  auto size = leaf_node -> GetSize();
  auto new_size = leaf_node -> Insert(key, value, comparator_);

  // duplicate key
  if(new_size == size) {
    buffer_pool_manager_ -> UnpinPage(leaf_page -> GetPageId(), false);
    return false;
  }

  // leaf not full
  if(new_size < leaf_max_size_) {
    buffer_pool_manager_ -> UnpinPage(leaf_page -> GetPageId(), true);
    return true;
  }

  auto *new_leaf_node = Split(leaf_node);
  InsertIntoParent(leaf_node, new_leaf_node -> KeyAt(0), new_leaf_node, transaction);
  buffer_pool_manager_ -> UnpinPage(leaf_page -> GetPageId(), true);
  buffer_pool_manager_ -> UnpinPage(new_leaf_node -> GetPageId(), true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key,
                                      BPlusTreePage *new_node, Transaction *transaction) {
  // 1. old_node 是根节点，整个树升高一层

  if(old_node -> IsRootPage()) {
    page_id_t new_page_id = INVALID_PAGE_ID;
    Page *new_page = buffer_pool_manager_ -> NewPage(&new_page_id);
    InternalPage *new_root_node = reinterpret_cast<InternalPage *>(new_page -> GetData());

    root_page_id_ = new_page_id;
    new_root_node -> Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root_node -> PopulateNewRoot(old_node -> GetPageId(), key, new_node -> GetPageId());

    old_node -> SetParentPageId(new_page_id);
    new_node -> SetParentPageId(new_page_id);

    UpdateRootPageId(0);
    return ;
  }

  // 2. old_node 不是根节点，找到 old_node 的父节点操作。
  Page *parent_page = buffer_pool_manager_ -> FetchPage(old_node -> GetParentPageId());
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page -> GetData());
  parent_node -> InsertAfterNode(old_node -> GetPageId(), key, new_node -> GetPageId());

  // number of children BEFORE insertion equals to max_size for internal nodes.
  if(parent_node -> GetSize() < parent_node -> GetMaxSize()) {
    buffer_pool_manager_ -> UnpinPage(parent_node -> GetPageId(), true);
    return ;
  }

  InternalPage *new_parent_node = Split(parent_node);
  InsertIntoParent(parent_node, new_parent_node -> KeyAt(0), new_parent_node, transaction);
  buffer_pool_manager_ -> UnpinPage(parent_node -> GetPageId(), true);
  buffer_pool_manager_ -> UnpinPage(new_parent_node -> GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t new_page_id = INVALID_PAGE_ID;
  auto *new_page = buffer_pool_manager_ -> NewPage(&new_page_id);
  if(new_page == nullptr) {
    throw std::runtime_error("Cannot create new page!");
  }
  auto *new_node = reinterpret_cast<N *>(new_page -> GetData());

  new_node -> SetPageType(node -> GetPageType());

  if(node -> IsLeafPage()) {
    auto *old_leaf_node = reinterpret_cast<LeafPage *>(node);
    auto *new_leaf_node = reinterpret_cast<LeafPage *>(new_node);
    new_leaf_node -> Init(new_page_id, old_leaf_node -> GetParentPageId(),leaf_max_size_);
    old_leaf_node -> MoveHalfTo(new_leaf_node);

    new_leaf_node -> SetNextPageId(old_leaf_node -> GetNextPageId());
    old_leaf_node -> SetNextPageId(new_leaf_node -> GetPageId());
    new_node = reinterpret_cast<N *>(new_leaf_node);
  } else {
    auto *old_internal_node = reinterpret_cast<InternalPage *>(node);
    auto *new_internal_node = reinterpret_cast<InternalPage *>(new_node);

    new_internal_node -> Init(new_page_id, old_internal_node -> GetParentPageId(), internal_max_size_);
    old_internal_node -> MoveHalfTo(new_internal_node);
    new_node = reinterpret_cast<N *>(old_internal_node);
  }

  return new_node;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return 0; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */

// header page 包含了 所有索引的名字 + root_id
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
