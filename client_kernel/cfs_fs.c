#include "cfs_fs.h"

#define CFS_FS_MAGIC 0x20230705
#define CFS_BLOCK_SIZE_SHIFT 12
#define CFS_BLOCK_SIZE (1UL << CFS_BLOCK_SIZE_SHIFT)
#define CFS_INODE_MAX_ID ((1UL << 63) - 1)

#define CFS_UPDATE_LIMIT_INTERVAL_MS 5 * 60 * 1000u
#define CFS_LINKS_DEFAULT 20000000
#define CFS_LINKS_MIN 1000000

#define pr_qstr(q) \
	(q) ? (q)->len : 0, (q) ? (q)->name : (const unsigned char *)""

#define fmt_inode "%p{.ino=%lu,.imode=0%o,.uid=%u}"
#define pr_inode(inode)                                                       \
	(inode), (inode) ? (inode)->i_ino : 0, (inode) ? (inode)->i_mode : 0, \
		(inode) ? i_uid_read(inode) : 0

#define fmt_file "%p{}"
#define pr_file(file) (file)

#define fmt_dentry "%p{.name=%.*s}"
#define pr_dentry(d)                    \
	(d), (d) ? (d)->d_name.len : 0, \
		(d) ? (d)->d_name.name : (const unsigned char *)""

// #define ENABLE_XATTR

static struct kmem_cache *inode_cache;
static struct kmem_cache *pagevec_cache;

#define CFS_INODE(i) ((struct cfs_inode *)(i))

struct cfs_inode {
	struct inode vfs_inode;
	unsigned long revalidate_jiffies;
	unsigned long iattr_jiffies;
	unsigned long quota_jiffies;
	struct cfs_extent_stream *es;
	char *link_target;
	struct cfs_quota_info_array quota_infos;
};

struct cfs_file_info {
	char *marker;
	struct cfs_packet_dentry_array denties;
	size_t denties_offset;
	bool done;
};

static inline void cfs_file_info_release(struct cfs_file_info *fi)
{
	if (!fi)
		return;
	if (fi->marker)
		kfree(fi->marker);
	cfs_packet_dentry_array_clear(&fi->denties);
	kfree(fi);
}

static inline bool is_iattr_cache_valid(struct cfs_inode *ci)
{
	struct super_block *sb = ci->vfs_inode.i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;

	return ci->iattr_jiffies +
		       msecs_to_jiffies(cmi->options->attr_cache_valid_ms) >
	       jiffies;
}

static inline void set_iattr_cache_valid(struct cfs_inode *ci)
{
	ci->iattr_jiffies = jiffies;
}

static inline bool is_dentry_cache_valid(struct cfs_inode *ci)
{
	struct super_block *sb = ci->vfs_inode.i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;

	return ci->revalidate_jiffies +
		       msecs_to_jiffies(cmi->options->dentry_cache_valid_ms) >
	       jiffies;
}

static inline void set_dentry_cache_valid(struct cfs_inode *ci)
{
	ci->revalidate_jiffies = jiffies;
}

static inline bool is_quota_cache_valid(struct cfs_inode *ci)
{
	struct super_block *sb = ci->vfs_inode.i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;

	return ci->quota_jiffies +
		       msecs_to_jiffies(cmi->options->quota_cache_valid_ms) >
	       jiffies;
}

static inline void set_quota_cache_valid(struct cfs_inode *ci)
{
	ci->quota_jiffies = jiffies;
}

static inline bool is_links_exceed_limit(struct cfs_inode *ci)
{
	struct super_block *sb = ci->vfs_inode.i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;

	return ci->vfs_inode.i_nlink >= atomic_long_read(&cmi->links_limit);
}

static inline void cfs_inode_refresh_unlock(struct cfs_inode *ci,
					    struct cfs_packet_inode *iinfo)
{
	struct inode *inode = &ci->vfs_inode;

	inode->i_mode = iinfo->mode;
	inode->i_ctime = iinfo->create_time;
	inode->i_atime = iinfo->access_time;
	inode->i_mtime = iinfo->modify_time;
	i_uid_write(inode, iinfo->uid);
	i_gid_write(inode, iinfo->gid);
	set_nlink(inode, iinfo->nlink);
	inode->i_generation = iinfo->generation;
	i_size_write(inode, iinfo->size);
	cfs_quota_info_array_clear(&ci->quota_infos);
	cfs_quota_info_array_move(&ci->quota_infos, &iinfo->quota_infos);
	if (ci->link_target)
		kfree(ci->link_target);
	ci->link_target = cfs_move(iinfo->target, NULL);
}

static int cfs_inode_refresh(struct cfs_inode *ci)
{
	struct super_block *sb = ci->vfs_inode.i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	struct cfs_packet_inode *iinfo;
	int ret;

	ret = cfs_meta_get(cmi->meta, ci->vfs_inode.i_ino, &iinfo);
	if (ret < 0)
		return ret;
	spin_lock(&ci->vfs_inode.i_lock);
	cfs_inode_refresh_unlock(ci, iinfo);
	set_iattr_cache_valid(ci);
	set_quota_cache_valid(ci);
	set_dentry_cache_valid(ci);
	spin_unlock(&ci->vfs_inode.i_lock);
	cfs_packet_inode_release(iinfo);
	return 0;
}

static struct inode *cfs_inode_new(struct super_block *sb,
				   struct cfs_packet_inode *iinfo, dev_t rdev)
{
	struct cfs_mount_info *cmi = sb->s_fs_info;
	struct cfs_inode *ci;
	struct inode *inode;

	ci = (struct cfs_inode *)iget_locked(sb, iinfo->ino);
	if (IS_ERR_OR_NULL(ci))
		return NULL;
	inode = &ci->vfs_inode;

	if (!(inode->i_state & I_NEW)) {
		cfs_log_debug("old inode %p{.ino=%lu, .iprivate=%p}\n", inode,
			      inode->i_ino, inode->i_private);
		return inode;
	}

	cfs_inode_refresh_unlock(ci, iinfo);
	set_dentry_cache_valid(ci);
	set_iattr_cache_valid(ci);
	set_quota_cache_valid(ci);

	/* timestamps updated by server */
	inode->i_flags |= S_NOATIME | S_NOCMTIME;
	inode->i_flags |= S_NOSEC;

	switch (inode->i_mode & S_IFMT) {
	case S_IFREG:
		inode->i_op = &cfs_file_iops;
		inode->i_fop = &cfs_file_fops;
		inode->i_data.a_ops = &cfs_address_ops;
		ci->es = cfs_extent_stream_new(cmi->ec, inode->i_ino);
		if (!ci->es) {
			iget_failed(inode);
			return NULL;
		}
		break;
	case S_IFDIR:
		inode->i_op = &cfs_dir_iops;
		inode->i_fop = &cfs_dir_fops;
		break;
	case S_IFLNK:
		inode->i_op = &cfs_symlink_iops;
		break;
	case S_IFIFO:
		inode->i_op = &cfs_special_iops;
		init_special_inode(inode, inode->i_mode, rdev);
		break;
	default:
		cfs_log_err("unsupport inode mode 0%o\n", inode->i_mode);
		break;
	}
	unlock_new_inode(inode);
	return inode;
}

struct cfs_pagevec {
	size_t nr;
	struct page *pages[CFS_PAGE_VEC_NUM + 32];
};

static inline struct cfs_pagevec *cfs_pagevec_new(void)
{
	return kmem_cache_zalloc(pagevec_cache, GFP_NOFS);
}

static inline void cfs_pagevec_release(struct cfs_pagevec *vec)
{
	kmem_cache_free(pagevec_cache, vec);
}

static bool cfs_pagevec_append(struct cfs_pagevec *vec, struct page *page)
{
	if (vec->nr == ARRAY_SIZE(vec->pages))
		return false;
	if (vec->nr > 0 && vec->pages[vec->nr - 1]->index + 1 != page->index)
		return false;
	vec->pages[vec->nr++] = page;
	return true;
}

static bool cfs_pagevec_empty(struct cfs_pagevec *vec)
{
	return vec->nr == 0;
}

static void cfs_pagevec_clear(struct cfs_pagevec *vec)
{
	vec->nr = 0;
}

/**
 * Called when readpages() failed to update page.
 */
static int cfs_readpage(struct file *file, struct page *page)
{
	struct inode *inode = file_inode(file);
	struct cfs_inode *ci = (struct cfs_inode *)inode;

	cfs_log_debug("file=" fmt_file ", page=%p{.index=%lu}\n", file, page,
		      page ? page->index : 0);
	return cfs_extent_read_pages(ci->es, &page, 1);
}

static int cfs_readpages_cb(void *data, struct page *page)
{
	struct cfs_pagevec *vec = data;
	struct inode *inode = page->mapping->host;
	struct cfs_inode *ci = (struct cfs_inode *)inode;
	int ret;

	if (!cfs_pagevec_append(vec, page)) {
		ret = cfs_extent_read_pages(ci->es, vec->pages, vec->nr);
		cfs_pagevec_clear(vec);
		if (ret < 0) {
			page_endio(page, READ, ret);
			return ret;
		}
		ret = cfs_pagevec_append(vec, page);
		BUG_ON(!ret);
	}
	return 0;
}

/**
 * Called by generic_file_aio_read(). Pages maybe discontinuous.
 */
static int cfs_readpages(struct file *file, struct address_space *mapping,
			 struct list_head *pages, unsigned nr_pages)
{
	struct inode *inode = file_inode(file);
	struct cfs_inode *ci = (struct cfs_inode *)inode;
	struct cfs_pagevec *vec;
	int ret;

	cfs_log_debug("file=%p{}, nr_pages=%u\n", file, nr_pages);

	vec = cfs_pagevec_new();
	if (!vec)
		return -ENOMEM;
	ret = read_cache_pages(mapping, pages, cfs_readpages_cb, vec);
	if (ret < 0)
		goto out;
	if (!cfs_pagevec_empty(vec))
		ret = cfs_extent_read_pages(ci->es, vec->pages, vec->nr);

out:
	cfs_pagevec_release(vec);
	return ret;
}

static inline loff_t cfs_inode_page_size(struct cfs_inode *ci,
					 struct page *page)
{
	loff_t offset = page_offset(page);

	return min((loff_t)PAGE_SIZE, i_size_read(&ci->vfs_inode) - offset);
}

static int cfs_writepage(struct page *page, struct writeback_control *wbc)
{
	struct inode *inode = page->mapping->host;
	struct cfs_inode *ci = (struct cfs_inode *)inode;
	loff_t page_size;

	page_size = cfs_inode_page_size(ci, page);
	set_page_writeback(page);
	return cfs_extent_write_pages(ci->es, &page, 1, page_size);
}

static int cfs_writepages_cb(struct page *page, struct writeback_control *wbc,
			     void *data)
{
	struct inode *inode = page->mapping->host;
	struct cfs_inode *ci = (struct cfs_inode *)inode;
	struct cfs_pagevec *vec = data;
	loff_t page_size;
	int ret;

	if (!cfs_pagevec_append(vec, page)) {
		page_size = cfs_inode_page_size(ci, vec->pages[vec->nr - 1]);
		ret = cfs_extent_write_pages(ci->es, vec->pages, vec->nr,
					     page_size);
		cfs_pagevec_clear(vec);
		if (ret < 0) {
			unlock_page(page);
			return ret;
		}
		ret = cfs_pagevec_append(vec, page);
		BUG_ON(!ret);
	}
	set_page_writeback(page);
	return 0;
}

/**
 * Called by flush()/fsync(). Pages maybe discontinuous.
 * Caller not holds the i_mutex.
 */
static int cfs_writepages(struct address_space *mapping,
			  struct writeback_control *wbc)
{
	struct inode *inode = mapping->host;
	struct cfs_inode *ci = (struct cfs_inode *)inode;
	struct cfs_pagevec *vec;
	loff_t page_size;
	int ret = 0;

	cfs_log_debug("inode=" fmt_inode "\n", pr_inode(inode));
	vec = cfs_pagevec_new();
	if (!vec)
		return -ENOMEM;
	write_cache_pages(mapping, wbc, cfs_writepages_cb, vec);
	if (!cfs_pagevec_empty(vec)) {
		page_size = cfs_inode_page_size(ci, vec->pages[vec->nr - 1]);
		ret = cfs_extent_write_pages(ci->es, vec->pages, vec->nr,
					     page_size);
	}
	cfs_pagevec_release(vec);
	return ret;
}

/**
 * Called by generic_file_aio_write(), caller holds the i_mutex.
 */
static int cfs_write_begin(struct file *file, struct address_space *mapping,
			   loff_t pos, unsigned len, unsigned flags,
			   struct page **pagep, void **fsdata)
{
	struct inode *inode = file_inode(file);
	struct cfs_inode *ci = (struct cfs_inode *)inode;
	pgoff_t index = pos >> PAGE_SHIFT;
	loff_t page_off = pos & PAGE_MASK;
	int pos_in_page = pos & ~PAGE_MASK;
	int end_in_page = pos_in_page + len;
	struct page *page;
	loff_t i_size;
	int ret;

	/**
	 * find or create a locked page.
	 */
	page = grab_cache_page_write_begin(mapping, index, flags);
	if (!page)
		return -ENOMEM;

	*pagep = page;

	/**
	 * 1. uptodate page write.
	 */
	if (PageUptodate(page))
		return 0;

	/**
	 * 2. full page write.
	 */
	if (pos_in_page == 0 && len == PAGE_SIZE)
		return 0;

	/**
	 * 3. end of file.
	 */
	i_size = i_size_read(inode);
	if (page_off >= i_size || (pos_in_page == 0 && (pos + len) >= i_size &&
				   end_in_page - pos_in_page != PAGE_SIZE)) {
		zero_user_segments(page, 0, pos_in_page, end_in_page,
				   PAGE_SIZE);
		return 0;
	}

	/**
	 * 4. uncached page write, page must be read from server first.
	 */
	ret = cfs_extent_read_pages(ci->es, &page, 1);
	lock_page(page);
	if (PageError(page))
		ret = -EIO;
	if (ret < 0) {
		unlock_page(page);
		put_page(page);
	}
	return ret;
}

/**
 * Called by generic_file_aio_write(), Caller holds the i_mutex.
 */
static int cfs_write_end(struct file *file, struct address_space *mapping,
			 loff_t pos, unsigned len, unsigned copied,
			 struct page *page, void *fsdata)
{
	struct inode *inode = page->mapping->host;
	loff_t last_pos = pos + copied;

	if (copied < len) {
		unsigned from = pos & (PAGE_SIZE - 1);

		zero_user(page, from + copied, len - copied);
	}

	if (!PageUptodate(page))
		SetPageUptodate(page);

	if (last_pos > i_size_read(inode))
		i_size_write(inode, last_pos);

	set_page_dirty(page);
	unlock_page(page);
	put_page(page);

	return copied;
}

/**
  * Called by generic_file_aio_write(), Caller holds the i_mutex.
  */
#if defined(KERNEL_HAS_DIO_WITH_ITER)
static ssize_t cfs_direct_io(struct kiocb *iocb, struct iov_iter *iter)
{
	struct file *file = iocb->ki_filp;
	struct inode *inode = file_inode(file);
	loff_t offset = iocb->ki_pos;

	if (iov_iter_rw(iter) == READ)
		return cfs_extent_dio_read(CFS_INODE(inode)->es, iter, offset);
	else
		return cfs_extent_dio_write(CFS_INODE(inode)->es, iter, offset);
	return -1;
}
#elif defined(KERNEL_HAS_DIO_WITH_ITER_AND_OFFSET)
static ssize_t cfs_direct_io(struct kiocb *iocb, struct iov_iter *iter,
			     loff_t offset)
{
	struct file *file = iocb->ki_filp;
	struct inode *inode = file_inode(file);

	if (iov_iter_rw(iter) == READ)
		return cfs_extent_dio_read(CFS_INODE(inode)->es, iter, offset);
	else
		return cfs_extent_dio_write(CFS_INODE(inode)->es, iter, offset);
	return -1;
}
#else
static ssize_t cfs_direct_io(int type, struct kiocb *iocb,
			     const struct iovec *iov, loff_t offset,
			     unsigned long nr_segs)
{
	struct file *file = iocb->ki_filp;
	struct inode *inode = file_inode(file);
	struct cfs_inode *ci = (struct cfs_inode *)inode;
	struct iov_iter iter;

	cfs_log_debug("file=" fmt_file
		      ", offset=%lld, nr_segs=%lu, iov_len=%zu\n",
		      pr_file(file), offset, nr_segs, iov_length(iov, nr_segs));

#ifdef KERNEL_HAS_IOV_ITER_WITH_TAG
	iov_iter_init(&ii, WRITE, iov, nr_segs, len);
#else
	iov_iter_init(&iter, iov, nr_segs, iov_length(iov, nr_segs), 0);
#endif
	switch (type) {
	case READ:
		return cfs_extent_dio_read(ci->es, &iter, offset);
	case WRITE:
		return cfs_extent_dio_write(ci->es, &iter, offset);
	default:
		return -1;
	}
}
#endif

static int cfs_open(struct inode *inode, struct file *file)
{
	struct cfs_file_info *cfi = file->private_data;

	cfs_log_debug("file=" fmt_file ", inode=" fmt_inode
		      ", dentry=" fmt_dentry "\n",
		      pr_file(file), pr_inode(inode),
		      pr_dentry(file_dentry(file)));

	if (cfi) {
		cfs_log_debug("open file %p is already opened\n", file);
		return 0;
	}
	cfi = kzalloc(sizeof(*cfi), GFP_NOFS);
	if (!cfi)
		return -ENOMEM;
	switch (inode->i_mode & S_IFMT) {
	case S_IFDIR:
		cfi->marker = kstrdup("", GFP_KERNEL);
		if (!cfi->marker) {
			kfree(cfi);
			return -ENOMEM;
		}
		break;

	default:
		break;
	}
	file->private_data = cfi;
#if defined(KERNEL_HAS_ITERATE_DIR) && defined(FMODE_KABI_ITERATE)
	file->f_mode |= FMODE_KABI_ITERATE;
#endif
	return 0;
}

static int cfs_release(struct inode *inode, struct file *file)
{
	struct cfs_file_info *cfi = file->private_data;

	cfs_log_debug("file=" fmt_file ", inode=" fmt_inode
		      ", dentry=" fmt_dentry "\n",
		      pr_file(file), pr_inode(inode),
		      pr_dentry(file_dentry(file)));

	if (!cfi)
		return 0;
	if (cfi->marker)
		kfree(cfi->marker);
	cfs_packet_dentry_array_clear(&cfi->denties);
	kfree(cfi);
	file->private_data = NULL;
	return 0;
}

static int cfs_flush(struct file *file, fl_owner_t id)
{
	struct inode *inode = file_inode(file);
	struct cfs_inode *ci = (struct cfs_inode *)inode;
	int ret;

	cfs_log_debug("file=" fmt_file "\n", pr_file(file));
	ret = write_inode_now(inode, 1);
	if (ret < 0)
		return ret;
	ret = filemap_fdatawait(file->f_mapping);
	if (ret < 0)
		return ret;
	return cfs_extent_stream_flush(ci->es);
}

static int cfs_fsync(struct file *file, loff_t start, loff_t end, int datasync)
{
	struct inode *inode = file_inode(file);
	struct cfs_inode *ci = (struct cfs_inode *)inode;
	int ret;

	cfs_log_debug("file=" fmt_file "\n", pr_file(file));
	ret = write_inode_now(inode, 1);
	if (ret < 0)
		return ret;
	ret = filemap_fdatawait_range(file->f_mapping, start, end);
	if (ret < 0)
		return ret;
	return cfs_extent_stream_flush(ci->es);
}

#define READDIR_NUM 1024
#if defined(KERNEL_HAS_ITERATE_DIR_SHARED) || defined(KERNEL_HAS_ITERATE_DIR)
static int cfs_iterate_dir(struct file *file, struct dir_context *ctx)
#else
static int cfs_readdir(struct file *file, void *dirent, filldir_t filldir)
#endif
{
	struct inode *inode = file_inode(file);
	struct super_block *sb = inode->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	struct cfs_file_info *cfi = file->private_data;
	struct cfs_packet_dentry *dentry;
	size_t i;
	int ret;

	cfs_log_debug("file=" fmt_file ", inode=" fmt_inode
		      ", dentry=" fmt_dentry
		      ", offset=%zu, nr_dentry=%zu, done=%d\n",
		      pr_file(file), pr_inode(inode),
		      pr_dentry(file_dentry(file)), cfi->denties_offset,
		      cfi->denties.num, cfi->done);

#if defined(KERNEL_HAS_ITERATE_DIR_SHARED) || defined(KERNEL_HAS_ITERATE_DIR)
	if (!dir_emit_dots(file, ctx))
		return 0;
	for (; cfi->denties_offset < cfi->denties.num; cfi->denties_offset++) {
		dentry = &cfi->denties.base[cfi->denties_offset];
		if (!dir_emit(ctx, dentry->name, strlen(dentry->name),
			      dentry->ino, (dentry->type >> 12) & 15)) {
			return 0;
		}
		ctx->pos++;
	}
#else
	if (file->f_pos == 0) {
		if (filldir(dirent, ".", 1, 0, inode->i_ino, DT_DIR) < 0)
			return 0;
		file->f_pos = 1;
	}
	if (file->f_pos == 1) {
		if (filldir(dirent, "..", 2, 1, parent_ino(file->f_path.dentry),
			    DT_DIR) < 0)
			return 0;
		file->f_pos = 2;
	}
	for (; cfi->denties_offset < cfi->denties.num; cfi->denties_offset++) {
		dentry = &cfi->denties.base[cfi->denties_offset];
		if (filldir(dirent, dentry->name, strlen(dentry->name),
			    file->f_pos, dentry->ino,
			    (dentry->type >> 12) & 15) < 0) {
			return 0;
		}
		file->f_pos++;
	}
#endif

	while (!cfi->done) {
		struct u64_array ino_vec;
		struct cfs_packet_inode_ptr_array iinfo_vec;

		if (cfi->denties.num > 0) {
			kfree(cfi->marker);
			cfi->marker = kstrdup(
				cfi->denties.base[cfi->denties.num - 1].name,
				GFP_KERNEL);
			if (!cfi->marker)
				return -ENOMEM;
			cfs_packet_dentry_array_clear(&cfi->denties);
		}
		ret = cfs_meta_readdir(cmi->meta, inode->i_ino, cfi->marker,
				       READDIR_NUM, &cfi->denties);
		if (ret < 0) {
			cfs_log_err("readdir error %d\n", ret);
			return ret;
		}
		if (cfi->denties.num < READDIR_NUM)
			cfi->done = true;

		ret = u64_array_init(&ino_vec, cfi->denties.num);
		if (ret < 0)
			return ret;
		ret = cfs_meta_batch_get(cmi->meta, &ino_vec, &iinfo_vec);
		u64_array_clear(&ino_vec);
		if (ret < 0)
			return ret;

		for (i = 0; i < iinfo_vec.num; i++) {
			struct cfs_inode *ci;

			ci = (struct cfs_inode *)ilookup(
				sb, iinfo_vec.base[i]->ino);
			if (!ci)
				continue;
			spin_lock(&ci->vfs_inode.i_lock);
			cfs_inode_refresh_unlock(ci, iinfo_vec.base[i]);
			set_iattr_cache_valid(ci);
			set_quota_cache_valid(ci);
			set_dentry_cache_valid(ci);
			spin_unlock(&ci->vfs_inode.i_lock);
			iput(&ci->vfs_inode);
		}
		/**
		 * free cfs_packet_inode array.
		 */
		cfs_packet_inode_ptr_array_clear(&iinfo_vec);

		for (cfi->denties_offset = 0;
		     cfi->denties_offset < cfi->denties.num;
		     cfi->denties_offset++) {
			dentry = &cfi->denties.base[cfi->denties_offset];
#if defined(KERNEL_HAS_ITERATE_DIR_SHARED) || defined(KERNEL_HAS_ITERATE_DIR)
			if (!dir_emit(ctx, dentry->name, strlen(dentry->name),
				      dentry->ino, (dentry->type >> 12) & 15)) {
				return 0;
			}
			ctx->pos++;
#else
			if (filldir(dirent, dentry->name, strlen(dentry->name),
				    file->f_pos, dentry->ino,
				    (dentry->type >> 12) & 15) < 0) {
				return 0;
			}
			file->f_pos++;
#endif
		}
	}
	return 0;
}

/**
 * Directory Entry Cache
 */
static int cfs_d_revalidate(struct dentry *dentry, unsigned int flags)
{
	struct super_block *sb = dentry->d_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	struct inode *inode = d_inode(dentry);
	struct cfs_inode *ci = (struct cfs_inode *)inode;
	int ret = 0;

	if (flags & LOOKUP_RCU)
		return -ECHILD;

	if (!inode)
		return true;

	if (!is_dentry_cache_valid(ci)) {
		ret = cfs_meta_get(cmi->meta, inode->i_ino, NULL);
		if (ret == -ENOENT) {
			set_dentry_cache_valid(ci);
			return false;
		} else if (ret < 0) {
			cfs_log_warning("get inode(%lu) error %d, try again\n",
					inode->i_ino, ret);
			return true;
		} else {
			set_dentry_cache_valid(ci);
			return true;
		}
	}
	return true;
}

/**
 * File Inode
 */
static int cfs_permission(struct inode *inode, int mask)
{
	if (mask & MAY_NOT_BLOCK)
		return -ECHILD;
	return generic_permission(inode, mask);
}

static int cfs_setattr(struct dentry *dentry, struct iattr *iattr)
{
	struct super_block *sb = dentry->d_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	struct inode *inode = d_inode(dentry);
	struct cfs_inode *ci = (struct cfs_inode *)inode;
	int err;

	cfs_log_debug("dentry=" fmt_dentry ", ia_valid=0x%x\n",
		      pr_dentry(dentry), iattr->ia_valid);

#ifdef KERNEL_HAS_SETATTR_PREPARE
	err = setattr_prepare(dentry, iattr);
#else
	err = inode_change_ok(inode, iattr);
#endif
	if (err)
		return err;

	if (iattr->ia_valid & ATTR_SIZE) {
		truncate_setsize(inode, iattr->ia_size);
		err = cfs_extent_stream_truncate(ci->es, iattr->ia_size);
		if (err)
			return err;
	}

	if (ia_valid_to_u32(iattr->ia_valid) != 0) {
		err = cfs_meta_set_attr(cmi->meta, inode->i_ino, iattr);
		if (err)
			return err;
	}

	setattr_copy(inode, iattr);
	mark_inode_dirty(inode);
	return 0;
}

#ifdef KERNEL_HAS_GETATTR_WITH_PATH
static int cfs_getattr(const struct path *path, struct kstat *stat,
		       u32 request_mask, unsigned int query_flags)
{
	struct inode *inode = d_inode(path->dentry);
	struct cfs_inode *ci = (struct cfs_inode *)inode;

	if (!is_iattr_cache_valid(ci))
		cfs_inode_refresh(ci);
	generic_fillattr(inode, stat);
	return 0;
}
#else
static int cfs_getattr(struct vfsmount *mnt, struct dentry *dentry,
		       struct kstat *stat)
{
	struct inode *inode = d_inode(dentry);
	struct cfs_inode *ci = (struct cfs_inode *)inode;

	if (!is_iattr_cache_valid(ci))
		cfs_inode_refresh(ci);
	generic_fillattr(inode, stat);
	return 0;
}
#endif

#ifdef ENABLE_XATTR
static int cfs_setxattr(struct dentry *dentry, const char *name,
			const void *value, size_t len, int flags)
{
	struct super_block *sb = dentry->d_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	u64 ino = dentry->d_inode->i_ino;

	cfs_log_debug("dentry=" fmt_dentry
		      ", name=%s, value=%.*s, flags=0x%x\n",
		      pr_dentry(dentry), name, (int)len, (const char *)value,
		      flags);
	return cfs_meta_set_xattr(cmi->meta, ino, name, value, len, flags);
}

static ssize_t cfs_getxattr(struct dentry *dentry, const char *name,
			    void *value, size_t size)
{
	struct super_block *sb = dentry->d_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	u64 ino = dentry->d_inode->i_ino;

	cfs_log_debug("dentry=" fmt_dentry ", name=%s\n", pr_dentry(dentry),
		      name);
	return cfs_meta_get_xattr(cmi->meta, ino, name, value, size);
}

static ssize_t cfs_listxattr(struct dentry *dentry, char *names, size_t size)
{
	struct super_block *sb = dentry->d_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	u64 ino = dentry->d_inode->i_ino;

	cfs_log_debug("dentry=" fmt_dentry "\n", pr_dentry(dentry));
	return cfs_meta_list_xattr(cmi->meta, ino, names, size);
}

static int cfs_removexattr(struct dentry *dentry, const char *name)
{
	struct super_block *sb = dentry->d_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	u64 ino = dentry->d_inode->i_ino;

	cfs_log_debug("dentry=" fmt_dentry ", name=%s\n", pr_dentry(dentry),
		      name);
	return cfs_meta_remove_xattr(cmi->meta, ino, name);
}
#endif

static struct dentry *cfs_lookup(struct inode *dir, struct dentry *dentry,
				 unsigned int flags)
{
	struct super_block *sb = dir->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	struct cfs_packet_inode *iinfo;
	struct inode *inode;
	int ret;

	if (unlikely(dentry->d_name.len > NAME_MAX))
		return ERR_PTR(-ENAMETOOLONG);

	cfs_log_debug("dir=" fmt_inode ", dentry=" fmt_dentry ", flags=0x%x\n",
		      pr_inode(dir), pr_dentry(dentry), flags);

	ret = cfs_meta_lookup(cmi->meta, dir->i_ino, &dentry->d_name, &iinfo);
	if (ret == -ENOENT) {
		d_add(dentry, NULL);
		return NULL;
	} else if (ret < 0) {
		cfs_log_err("lookup inode '%.*s', error %d\n",
			    pr_qstr(&dentry->d_name), ret);
		return ERR_PTR(ret);
	}

	inode = cfs_inode_new(sb, iinfo, 0);
	cfs_packet_inode_release(iinfo);
	if (!inode) {
		cfs_log_err("create inode '%.*s' failed\n",
			    pr_qstr(&dentry->d_name));
		d_add(dentry, NULL);
		return NULL;
	}
	return d_splice_alias(inode, dentry);
}

static int cfs_create(struct inode *dir, struct dentry *dentry, umode_t mode,
		      bool excl)
{
	struct super_block *sb = dir->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	uid_t uid = from_kuid(&init_user_ns, current_fsuid());
	uid_t gid = from_kgid(&init_user_ns, current_fsgid());
	struct cfs_quota_info_array *quota = NULL;
	struct cfs_packet_inode *iinfo;
	struct inode *inode;
	int ret;

	cfs_log_debug("dir=" fmt_inode ", dentry=" fmt_dentry
		      ", mode=0%o, uid=%u, gid=%u\n",
		      pr_inode(dir), pr_dentry(dentry), mode, uid, gid);

	ret = cfs_inode_refresh(CFS_INODE(dir));
	if (ret < 0)
		return ret;

	if (is_links_exceed_limit(CFS_INODE(dir)))
		return -EDQUOT;

	if (cmi->options->enable_quota) {
		if (!is_quota_cache_valid(CFS_INODE(dir)))
			cfs_inode_refresh(CFS_INODE(dir));
		quota = &CFS_INODE(dir)->quota_infos;
	}

	ret = cfs_meta_create(cmi->meta, dir->i_ino, &dentry->d_name, mode, uid,
			      gid, NULL, quota, &iinfo);
	if (ret < 0) {
		cfs_log_err("create dentry error %d\n", ret);
		return ret;
	}

	inode = cfs_inode_new(sb, iinfo, 0);
	cfs_packet_inode_release(iinfo);
	if (!inode)
		return -ENOMEM;

	d_instantiate(dentry, inode);
	return 0;
}

static int cfs_link(struct dentry *src_dentry, struct inode *dst_dir,
		    struct dentry *dst_dentry)
{
	struct super_block *sb = dst_dir->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	int ret;

	cfs_log_debug("src_dentry=" fmt_dentry ", dst_dir=" fmt_inode
		      ", dst_dentry=" fmt_dentry "\n",
		      pr_dentry(src_dentry), pr_inode(dst_dir),
		      pr_dentry(dst_dentry));

	ret = cfs_inode_refresh(CFS_INODE(dst_dir));
	if (ret < 0)
		return ret;

	if (is_links_exceed_limit(CFS_INODE(dst_dir)))
		return -EDQUOT;

	ret = cfs_meta_link(cmi->meta, dst_dir->i_ino, &dst_dentry->d_name,
			    src_dentry->d_inode->i_ino, NULL);
	if (ret < 0) {
		d_drop(dst_dentry);
		return ret;
	}
	ihold(src_dentry->d_inode);
	d_instantiate(dst_dentry, src_dentry->d_inode);
	return 0;
}

static int cfs_symlink(struct inode *dir, struct dentry *dentry,
		       const char *target)
{
	struct super_block *sb = dir->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	uid_t uid = from_kuid(&init_user_ns, current_fsuid());
	uid_t gid = from_kgid(&init_user_ns, current_fsgid());
	struct cfs_quota_info_array *quota = NULL;
	umode_t mode;
	struct cfs_packet_inode *iinfo;
	struct inode *inode;
	int ret;

	cfs_log_debug("dir=" fmt_inode ", dentry=" fmt_dentry
		      ", target=%s, uid=%u, gid=%u\n",
		      pr_inode(dir), pr_dentry(dentry), target, uid, gid);

	ret = cfs_inode_refresh(CFS_INODE(dir));
	if (ret < 0)
		return ret;

	if (is_links_exceed_limit(CFS_INODE(dir)))
		return -EDQUOT;

	if (cmi->options->enable_quota) {
		if (!is_quota_cache_valid(CFS_INODE(dir)))
			cfs_inode_refresh(CFS_INODE(dir));
		quota = &CFS_INODE(dir)->quota_infos;
	}

	mode = S_IFLNK | S_IRWXU | S_IRWXG | S_IRWXO;
	ret = cfs_meta_create(cmi->meta, dir->i_ino, &dentry->d_name, mode, uid,
			      gid, target, quota, &iinfo);
	if (ret < 0) {
		cfs_log_err("create dentry error %d\n", ret);
		return ret;
	}

	inode = cfs_inode_new(sb, iinfo, 0);
	cfs_packet_inode_release(iinfo);
	if (!inode)
		return -ENOMEM;

	d_instantiate(dentry, inode);
	return 0;
}

static int cfs_mkdir(struct inode *dir, struct dentry *dentry, umode_t mode)
{
	struct super_block *sb = dir->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	uid_t uid = from_kuid(&init_user_ns, current_fsuid());
	uid_t gid = from_kgid(&init_user_ns, current_fsgid());
	struct cfs_quota_info_array *quota = NULL;
	struct cfs_packet_inode *iinfo;
	struct inode *inode;
	int ret;

	cfs_log_debug("dir=" fmt_inode ", dentry=" fmt_dentry
		      ", mode=0%o, uid=%u, gid=%u\n",
		      pr_inode(dir), pr_dentry(dentry), mode, uid, gid);

	ret = cfs_inode_refresh(CFS_INODE(dir));
	if (ret < 0)
		return ret;

	if (is_links_exceed_limit(CFS_INODE(dir)))
		return -EDQUOT;

	if (cmi->options->enable_quota) {
		if (!is_quota_cache_valid(CFS_INODE(dir)))
			cfs_inode_refresh(CFS_INODE(dir));
		quota = &CFS_INODE(dir)->quota_infos;
	}

	mode &= ~current_umask();
	mode |= S_IFDIR;
	ret = cfs_meta_create(cmi->meta, dir->i_ino, &dentry->d_name, mode, uid,
			      gid, NULL, quota, &iinfo);
	if (ret < 0) {
		cfs_log_err("create dentry error %d\n", ret);
		return ret;
	}

	inode = cfs_inode_new(sb, iinfo, 0);
	cfs_packet_inode_release(iinfo);
	if (!inode)
		return -ENOMEM;

	d_instantiate(dentry, inode);
	return 0;
}

static int cfs_rmdir(struct inode *dir, struct dentry *dentry)
{
	struct super_block *sb = dir->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	int ret;

	cfs_log_debug("dir=" fmt_inode ", dentry=" fmt_dentry "\n",
		      pr_inode(dir), pr_dentry(dentry));
	ret = cfs_meta_delete(cmi->meta, dir->i_ino, &dentry->d_name,
			      d_is_dir(dentry));
	if (ret < 0)
		cfs_log_debug("dir=" fmt_inode ", dentry=" fmt_dentry
			      ", error=%d\n",
			      pr_inode(dir), pr_dentry(dentry), ret);
	return ret;
}

static int cfs_mknod(struct inode *dir, struct dentry *dentry, umode_t mode,
		     dev_t rdev)
{
	struct super_block *sb = dir->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	uid_t uid = from_kuid(&init_user_ns, current_fsuid());
	uid_t gid = from_kgid(&init_user_ns, current_fsgid());
	struct cfs_quota_info_array *quota = NULL;
	struct cfs_packet_inode *iinfo;
	struct inode *inode;
	int ret;

	cfs_log_debug("dir=" fmt_inode ", dentry=" fmt_dentry
		      ", mode=0%o, uid=%u, gid=%u\n",
		      pr_inode(dir), pr_dentry(dentry), mode, uid, gid);

	ret = cfs_inode_refresh(CFS_INODE(dir));
	if (ret < 0)
		return ret;

	if (is_links_exceed_limit(CFS_INODE(dir)))
		return -EDQUOT;

	if (cmi->options->enable_quota) {
		if (!is_quota_cache_valid(CFS_INODE(dir)))
			cfs_inode_refresh(CFS_INODE(dir));
		quota = &CFS_INODE(dir)->quota_infos;
	}

	mode &= ~current_umask();
	ret = cfs_meta_create(cmi->meta, dir->i_ino, &dentry->d_name, mode, uid,
			      gid, NULL, NULL, &iinfo);
	if (ret < 0) {
		cfs_log_err("create dentry error %d\n", ret);
		return ret;
	}
	inode = cfs_inode_new(sb, iinfo, rdev);
	cfs_packet_inode_release(iinfo);
	if (!inode)
		return -ENOMEM;

	d_instantiate(dentry, inode);
	return 0;
}

#ifdef KERNEL_HAS_RENAME_WITH_FLAGS
static int cfs_rename(struct inode *old_dir, struct dentry *old_dentry,
		      struct inode *new_dir, struct dentry *new_dentry,
		      unsigned int flags)
#else
static int cfs_rename(struct inode *old_dir, struct dentry *old_dentry,
		      struct inode *new_dir, struct dentry *new_dentry)
#endif
{
	struct super_block *sb = old_dir->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	int ret;

	cfs_log_debug("old_dir=" fmt_inode ", old_dentry=" fmt_dentry
		      ",new_dir = " fmt_inode ",new_dentry=" fmt_dentry "\n",
		      pr_inode(old_dir), pr_dentry(old_dentry),
		      pr_inode(new_dir), pr_dentry(new_dentry));

	/* Any flags not handled by the filesystem should result in EINVAL being returned */
#ifdef KERNEL_HAS_RENAME_WITH_FLAGS
	if (flags != 0)
		return -EINVAL;
#endif

	ret = cfs_inode_refresh(CFS_INODE(new_dir));
	if (ret < 0)
		return ret;

	if (is_links_exceed_limit(CFS_INODE(new_dir)))
		return -EDQUOT;

	return cfs_meta_rename(cmi->meta, old_dir->i_ino, &old_dentry->d_name,
			       new_dir->i_ino, &new_dentry->d_name, true);
}

static int cfs_unlink(struct inode *dir, struct dentry *dentry)
{
	struct super_block *sb = dir->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;

	cfs_log_debug("dir=" fmt_inode ", dentry=" fmt_dentry "\n",
		      pr_inode(dir), pr_dentry(dentry));
	return cfs_meta_delete(cmi->meta, dir->i_ino, &dentry->d_name,
			       d_is_dir(dentry));
}

/**
 * follow_link() is replaced with get_link().
 */
#ifdef KERNEL_HAS_GET_LINK
static const char *cfs_get_link(struct dentry *dentry, struct inode *inode,
				struct delayed_call *done)
{
	return inode->i_link;
}
#else
static void *cfs_follow_link(struct dentry *dentry, struct nameidata *nd)
{
	struct inode *inode = d_inode(dentry);
	struct cfs_inode *ci = (struct cfs_inode *)inode;

	cfs_log_debug("dentry=" fmt_dentry "\n", pr_dentry(dentry));
	nd_set_link(nd, ci->link_target);
	return NULL;
}
#endif

static struct inode *cfs_alloc_inode(struct super_block *sb)
{
	struct cfs_inode *ci;
	struct inode *inode;

	ci = kmem_cache_alloc(inode_cache, GFP_NOFS);
	if (!ci)
		return NULL;
	inode = &ci->vfs_inode;
	if (unlikely(inode_init_always(sb, inode))) {
		kmem_cache_free(inode_cache, ci);
		return NULL;
	}
	memset(&ci->quota_infos, 0, sizeof(ci->quota_infos));
	ci->link_target = NULL;
	ci->es = 0;
	return (struct inode *)ci;
}

static void cfs_destroy_inode(struct inode *inode)
{
	struct cfs_inode *ci = (struct cfs_inode *)inode;

	if (ci->link_target)
		kfree(ci->link_target);
	cfs_extent_stream_release(ci->es);
	cfs_quota_info_array_clear(&ci->quota_infos);
	kmem_cache_free(inode_cache, ci);
}

static int cfs_drop_inode(struct inode *inode)
{
	return generic_drop_inode(inode);
}

static void cfs_put_super(struct super_block *sb)
{
	struct cfs_mount_info *cmi = sb->s_fs_info;

	cfs_log_debug("sb=%p{.s_fs_info=%p}\n", sb, sb->s_fs_info);
	cfs_mount_info_release(cmi);
}

static int cfs_statfs(struct dentry *dentry, struct kstatfs *kstatfs)
{
	struct super_block *sb = dentry->d_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	struct cfs_volume_stat stat;
	int ret;

	cfs_log_info("dentry=" fmt_dentry "\n", pr_dentry(dentry));
	ret = cfs_master_get_volume_stat(cmi->master, &stat);
	if (ret < 0)
		return ret;
	memset(kstatfs, 0, sizeof(*kstatfs));
	kstatfs->f_type = CFS_FS_MAGIC;
	kstatfs->f_namelen = NAME_MAX;
	kstatfs->f_bsize = CFS_BLOCK_SIZE;
	kstatfs->f_frsize = CFS_BLOCK_SIZE;
	kstatfs->f_blocks = stat.total_size >> CFS_BLOCK_SIZE_SHIFT;
	kstatfs->f_bfree = (stat.total_size - stat.used_size) >>
			   CFS_BLOCK_SIZE_SHIFT;
	kstatfs->f_bavail = (stat.total_size - stat.used_size) >>
			    CFS_BLOCK_SIZE_SHIFT;
	kstatfs->f_files = stat.inode_count;
	kstatfs->f_ffree = CFS_INODE_MAX_ID - stat.inode_count;
	cfs_volume_stat_clear(&stat);
	return 0;
}

static int cfs_show_options(struct seq_file *seq_file, struct dentry *dentry)
{
	struct super_block *sb = dentry->d_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;

	seq_printf(seq_file, ",owner=%s", cmi->options->owner);
	seq_printf(seq_file, ",dentry_cache_valid_ms=%u",
		   cmi->options->dentry_cache_valid_ms);
	seq_printf(seq_file, ",attr_cache_valid_ms=%u",
		   cmi->options->attr_cache_valid_ms);
	seq_printf(seq_file, ",quota_cache_valid_ms=%u",
		   cmi->options->quota_cache_valid_ms);
	seq_printf(seq_file, ",enable_quota=%s",
		   cmi->options->enable_quota ? "true" : "false");
	return 0;
}

/**
 * Filesystem
 */

static int cfs_fs_fill_super(struct super_block *sb, void *data, int silent)
{
	struct cfs_mount_info *cmi = data;
	struct cfs_packet_inode *iinfo;
	struct inode *inode;
	int ret;

	sb->s_fs_info = cmi;
	sb->s_blocksize = CFS_BLOCK_SIZE;
	sb->s_blocksize_bits = CFS_BLOCK_SIZE_SHIFT;
	sb->s_maxbytes = MAX_LFS_FILESIZE;
	sb->s_magic = CFS_FS_MAGIC;
	sb->s_op = &cfs_super_ops;
	sb->s_d_op = &cfs_dentry_ops;
	sb->s_time_gran = 1;
	/* no acl */
	sb->s_flags |= MS_POSIXACL;

	ret = cfs_meta_lookup_path(cmi->meta, cmi->options->path, &iinfo);
	if (ret < 0)
		return ret;
	if (!S_ISDIR(iinfo->mode)) {
		cfs_packet_inode_release(iinfo);
		return -ENOTDIR;
	}
	/* root inode */
	inode = cfs_inode_new(sb, iinfo, 0);
	cfs_packet_inode_release(iinfo);
	if (!inode)
		return -ENOMEM;
	sb->s_root = d_make_root(inode);
	return 0;
}

/**
 * mount -t cubefs -o owner=ltptest //172.16.1.101:17010,172.16.1.102:17010,172.16.1.103:17010/ltptest /mnt/cubefs
 */
static struct dentry *cfs_mount(struct file_system_type *fs_type, int flags,
				const char *dev_str, void *opt_str)
{
	struct cfs_options *options;
	struct cfs_mount_info *cmi;
	struct dentry *dentry;

	cfs_log_info("dev=\"%s\", options=\"%s\"\n", dev_str, (char *)opt_str);

	options = cfs_options_new(dev_str, opt_str);
	if (IS_ERR(options))
		return ERR_CAST(options);
	cmi = cfs_mount_info_new(options);
	if (IS_ERR(cmi))
		return ERR_CAST(cmi);
	dentry = mount_nodev(fs_type, flags, cmi, cfs_fs_fill_super);
	if (IS_ERR(dentry))
		cfs_mount_info_release(cmi);
	return dentry;
}

static void cfs_kill_sb(struct super_block *sb)
{
	cfs_log_debug("sb=%p{.s_fs_info=%p}\n", sb, sb->s_fs_info);
	kill_anon_super(sb);
}

const struct address_space_operations cfs_address_ops = {
	.readpage = cfs_readpage,
	.readpages = cfs_readpages,
	.writepage = cfs_writepage,
	.writepages = cfs_writepages,
	.write_begin = cfs_write_begin,
	.write_end = cfs_write_end,
	.set_page_dirty = __set_page_dirty_nobuffers,
	.invalidatepage = NULL,
	.releasepage = NULL,
	.direct_IO = cfs_direct_io,
};

const struct file_operations cfs_file_fops = {
	.open = cfs_open,
	.release = cfs_release,
	.llseek = generic_file_llseek,
#ifdef KERNEL_HAS_READ_WRITE_ITER
	.read_iter = generic_file_read_iter,
	.write_iter = generic_file_write_iter,
#else
	.aio_read = generic_file_aio_read,
	.aio_write = generic_file_aio_write,
#endif
	.mmap = generic_file_mmap,
	.fsync = cfs_fsync,
	.flush = cfs_flush,
};

const struct inode_operations cfs_file_iops = {
	.permission = cfs_permission,
	.setattr = cfs_setattr,
	.getattr = cfs_getattr,
#ifdef ENABLE_XATTR
	.setxattr = cfs_setxattr,
	.getxattr = cfs_getxattr,
	.listxattr = cfs_listxattr,
	.removexattr = cfs_removexattr,
#endif
};

const struct file_operations cfs_dir_fops = {
	.open = cfs_open,
	.release = cfs_release,
	.read = generic_read_dir,
#ifdef KERNEL_HAS_ITERATE_DIR_SHARED
	.iterate_shared = cfs_iterate_dir,
#elif defined(KERNEL_HAS_ITERATE_DIR)
	.iterate = cfs_iterate_dir,
#else
	.readdir = cfs_readdir,
#endif
	.llseek = NULL,
	.fsync = noop_fsync,
};

const struct inode_operations cfs_dir_iops = {
	.lookup = cfs_lookup,
	.create = cfs_create,
	.link = cfs_link,
	.symlink = cfs_symlink,
	.mkdir = cfs_mkdir,
	.rmdir = cfs_rmdir,
	.mknod = cfs_mknod,
	.rename = cfs_rename,
	.unlink = cfs_unlink,
	.permission = cfs_permission,
	.setattr = cfs_setattr,
	.getattr = cfs_getattr,
#ifdef ENABLE_XATTR
	.setxattr = cfs_setxattr,
	.getxattr = cfs_getxattr,
	.listxattr = cfs_listxattr,
	.removexattr = cfs_removexattr,
#endif
};

const struct inode_operations cfs_symlink_iops = {
#ifdef KERNEL_HAS_GET_LINK
	.get_link = cfs_get_link,
#else
	.readlink = generic_readlink,
	.follow_link = cfs_follow_link,
#endif
};

const struct inode_operations cfs_special_iops = {
	.setattr = cfs_setattr,
	.getattr = cfs_getattr,
};

const struct dentry_operations cfs_dentry_ops = {
	.d_revalidate = cfs_d_revalidate,
};

const struct super_operations cfs_super_ops = {
	.alloc_inode = cfs_alloc_inode,
	.destroy_inode = cfs_destroy_inode,
	.drop_inode = cfs_drop_inode,
	.put_super = cfs_put_super,
	.statfs = cfs_statfs,
	.show_options = cfs_show_options,
};

struct file_system_type cfs_fs_type = {
	.name = "cubefs",
	.owner = THIS_MODULE,
	.kill_sb = cfs_kill_sb,
	.mount = cfs_mount,
};

static void update_limit_work_cb(struct work_struct *work)
{
	struct delayed_work *delayed_work = to_delayed_work(work);
	struct cfs_mount_info *cmi = container_of(
		delayed_work, struct cfs_mount_info, update_limit_work);
	struct cfs_cluster_info info;
	int ret;

	schedule_delayed_work(delayed_work,
			      msecs_to_jiffies(CFS_UPDATE_LIMIT_INTERVAL_MS));
	ret = cfs_master_get_cluster_info(cmi->master, &info);
	if (ret < 0) {
		cfs_log_err("get cluster info error %d\n", ret);
		return;
	}
	if (info.links_limit < CFS_LINKS_MIN)
		info.links_limit = CFS_LINKS_DEFAULT;
	atomic_long_set(&cmi->links_limit, info.links_limit);
	cfs_cluster_info_clear(&info);
}

/**
 * @return mount_info if success, error code if failed.
 */
struct cfs_mount_info *cfs_mount_info_new(struct cfs_options *options)
{
	struct cfs_mount_info *cmi;
	void *err_ptr;

	cmi = kzalloc(sizeof(*cmi), GFP_NOFS);
	if (!cmi) {
		cfs_options_release(options);
		return ERR_PTR(-ENOMEM);
	}
	cmi->options = options;
	atomic_long_set(&cmi->links_limit, CFS_LINKS_DEFAULT);
	INIT_DELAYED_WORK(&cmi->update_limit_work, update_limit_work_cb);
	cmi->master = cfs_master_client_new(&options->addrs, options->volume);
	if (!cmi->master) {
		err_ptr = ERR_PTR(-ENOMEM);
		goto err_master;
	}
	cmi->meta = cfs_meta_client_new(cmi->master, options->volume);
	if (IS_ERR(cmi->meta)) {
		err_ptr = ERR_CAST(cmi->meta);
		goto err_meta;
	}
	cmi->ec = cfs_extent_client_new(cmi->master, cmi->meta);
	if (IS_ERR(cmi->ec)) {
		err_ptr = ERR_CAST(cmi->ec);
		goto err_ec;
	}
	schedule_delayed_work(&cmi->update_limit_work, 0);
	return cmi;

err_ec:
	cfs_meta_client_release(cmi->meta);
err_meta:
	cfs_master_client_release(cmi->master);
err_master:
	cfs_options_release(cmi->options);
	kfree(cmi);
	return err_ptr;
}

void cfs_mount_info_release(struct cfs_mount_info *cmi)
{
	if (!cmi)
		return;
	cancel_delayed_work_sync(&cmi->update_limit_work);
	cfs_extent_client_release(cmi->ec);
	cfs_meta_client_release(cmi->meta);
	cfs_master_client_release(cmi->master);
	cfs_options_release(cmi->options);
	kfree(cmi);
}

static void init_once(void *foo)
{
	struct cfs_inode *ci = (struct cfs_inode *)foo;

	inode_init_once(&ci->vfs_inode);
}

int cfs_fs_module_init(void)
{
	if (!inode_cache) {
		inode_cache = kmem_cache_create(
			"cfs_inode", sizeof(struct cfs_inode),
			__alignof__(struct cfs_inode),
			(SLAB_RECLAIM_ACCOUNT | SLAB_MEM_SPREAD), init_once);
		if (!inode_cache)
			goto oom;
	}
	if (!pagevec_cache) {
		pagevec_cache = KMEM_CACHE(cfs_pagevec, SLAB_MEM_SPREAD);
		if (!pagevec_cache)
			goto oom;
	}
	return 0;

oom:
	cfs_fs_module_exit();
	return -ENOMEM;
}

void cfs_fs_module_exit(void)
{
	if (inode_cache) {
		kmem_cache_destroy(inode_cache);
		inode_cache = NULL;
	}
	if (pagevec_cache) {
		kmem_cache_destroy(pagevec_cache);
		pagevec_cache = NULL;
	}
}
