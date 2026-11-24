//! Format magics (`src/binary.js` plus the module-local ones).

pub const POSTING_SEGMENT_MAGIC: &[u8; 4] = b"RFPS";
pub const DIRECTORY_ROOT_MAGIC: &[u8; 4] = b"RFDR";
pub const DIRECTORY_PAGE_MAGIC: &[u8; 4] = b"RFDP";
pub const CODE_MAGIC: &[u8; 4] = b"RFCB";
pub const DOC_VALUE_MAGIC: &[u8; 4] = b"RFVB";
pub const FACET_DICT_MAGIC: &[u8; 4] = b"RFFD";
pub const DOC_POINTER_PAGE_MAGIC: &[u8; 4] = b"RFPD";
pub const DOC_PAGE_POINTER_MAGIC: &[u8; 4] = b"RFPG";
pub const DOC_PAGE_PAYLOAD_MAGIC: &[u8; 4] = b"RFPC";
pub const DOC_VALUE_SORT_DIRECTORY_MAGIC: &[u8; 4] = b"RFDT";
pub const DOC_VALUE_SORT_PAGE_MAGIC: &[u8; 4] = b"RFDV";
pub const QUERY_BUNDLE_MAGIC: &[u8; 4] = b"RFQB";
pub const AUTHORITY_SHARD_MAGIC: &[u8; 4] = b"RFAU";
pub const AUTHORITY_LEXICON_MAGIC: &[u8; 4] = b"RFLX";
pub const AUTHORITY_LEXICON_SEGMENT_MAGIC: &[u8; 4] = b"RFLP";
pub const AUTHORITY_HOT_MAGIC: &[u8; 4] = b"RFLH";
pub const GEO_TREE_ROOT_MAGIC: &[u8; 4] = b"RFGR";
pub const GEO_BRANCH_PAGE_MAGIC: &[u8; 4] = b"RFGB";
pub const GEO_LEAF_PAGE_MAGIC: &[u8; 4] = b"RFGL";
pub const VECTOR_ROOT_MAGIC: &[u8; 4] = b"RFVR";
pub const VECTOR_CLUSTER_PAGE_MAGIC: &[u8; 4] = b"RFVC";
/// `src/doc_value_manifest.js`
pub const DOC_VALUE_MANIFEST_MAGIC: &[u8; 4] = b"RFVM";
/// `src/filter_bitmaps.js`
pub const FILTER_BITMAP_MAGIC: &[u8; 4] = b"RFBM";
/// `src/segment_codec.js`
pub const SEGMENT_TERMS_MAGIC: &[u8; 8] = b"RFSGTER1";
/// `src/text_routing_codec.js`
pub const TEXT_ROUTING_SEGMENT_MAGIC: &[u8; 4] = b"RFTR";

/// `assertMagic` from `src/codec.js`: compares `bytes[i] !== magic[i]`, so a
/// short buffer fails too.
pub fn check_magic(bytes: &[u8], magic: &[u8], message: &str) -> crate::Result<()> {
    crate::util::bytes::assert_magic(bytes, magic, message)
}
