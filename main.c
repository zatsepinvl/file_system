#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>

#include <stdlib.h>
#include <unistd.h>

#define BLOCK_NUMBER 65536
#define BLOCK_SIZE 2048
#define FILENAME_LENGTH 100
#define FILE_NUMBER 1024

#define E_NES 1000
#define E_NFB 1001
#define E_NFD 1002
#define E_FNE 1003

#define FS_FILE_PATH "fs"

typedef struct file_descr {
    char not_free;
    char is_folder;
    int start;
    int size;
    char name[FILENAME_LENGTH];
} fd_t;

//block is free: list[i] == -2
//block points to another block: list[i] == n
//block contains end of file: list[i] == -1

typedef struct fs {
    int list[BLOCK_NUMBER];
    fd_t fd[FILE_NUMBER];
} fs_t;

fs_t fs;


//---------load fs-------------//
int init_fs() {
    int i = 0;
    for (i = 0; i < sizeof(fs.fd) / sizeof(fs.fd[0]); i++) {
        fs.fd[i].not_free = 0;
        fs.fd[i].is_folder = 0;
        memset(fs.fd[i].name, '\0', FILENAME_LENGTH);
        fs.fd[i].start = -1;
        fs.fd[i].size = 0;
    }
    for (i = 0; i < BLOCK_NUMBER; i++) {
        fs.list[i] = -2;
    }
    return 0;
}

int load_fs() {
    printf("Reading fs file...");
    int i = 0;
    init_fs();
    FILE *pfile = fopen(FS_FILE_PATH, "r");
    fread(fs.fd, sizeof(fd_t), FILE_NUMBER, pfile);
    fread(fs.list, sizeof(int), BLOCK_NUMBER, pfile);
    for (i = 0; i < BLOCK_NUMBER; i++)
        if (fs.list[i] == 0)
            fs.list[i] = -2;
    fclose(pfile);
    printf("Done\n");
    return 0;
}

print_fslist() {
    int i = 0;
    for (i = 0; i < BLOCK_NUMBER; i++)
        printf("%d\n", fs.list[i]);
}
//------------------//


int find_free_block() {
    int i = 0;
    for (i = 0; i < BLOCK_NUMBER; i++) {
        if (fs.list[i] == -2)
            return i;
    }
    return E_NFB;
}

int find_free_fd_id() {
    int i = 0;
    for (i = 0; i < FILE_NUMBER; i++) {
        if (!fs.fd[i].not_free)
            return i;
    }
    return E_NFD;
}


int write_fd(fd_t *fd, int id) {
    FILE *pfile = fopen(FS_FILE_PATH, "r+");
    fseek(pfile, id * sizeof(fd_t), SEEK_SET);
    fwrite(fd, 1, sizeof(fd_t), pfile);
    fclose(pfile);
    return 0;
}

int write_precedence_vector(fd_t *fd) {
    FILE *pfile = fopen(FS_FILE_PATH, "r+");
    int i = fd->start;
    fseek(pfile, sizeof(fd_t) * FILE_NUMBER + i * sizeof(int), SEEK_SET);
    fwrite(&fs.list[i], sizeof(int), 1, pfile);
    i = fs.list[i];
    while (i != -1) {
        fseek(pfile, sizeof(fd_t) * FILE_NUMBER + i * sizeof(int), SEEK_SET);
        fwrite(&fs.list[i], sizeof(int), 1, pfile);
        i = fs.list[i];
    }
    fclose(pfile);
    return 0;
}

int clear_precendence_vector(fd_t *fd) {
    FILE *pfile = fopen(FS_FILE_PATH, "r+");
    int i = fs.list[fd->start];
    while (i != -1) {
        int temp = fs.list[i];
        fs.list[i] = -2;
        fseek(pfile, sizeof(fd_t) * FILE_NUMBER + i * sizeof(int), SEEK_SET);
        fwrite(&fs.list[i], sizeof(int), 1, pfile);
        i = temp;
    }
    fseek(pfile, sizeof(fd_t) * FILE_NUMBER + fd->start * sizeof(int), SEEK_SET);
    fs.list[fd->start] = -1;
    fwrite(&fs.list[fd->start], sizeof(int), 1, pfile);
    fclose(pfile);
    return 0;
}

int add_file(char *name, char is_folder) {
    fd_t *fd = NULL;
    int start = 0;
    int fd_id = find_free_fd_id();
    if (fd_id == E_NFD)
        return E_NFD;
    fd = &fs.fd[fd_id];
    fd->not_free = 1;
    fd->is_folder = is_folder;
    fd->size = 0;
    strcpy(fd->name, name);
    start = find_free_block();
    if (start == E_NFB)
        return E_NFB;
    fd->start = start;
    fs.list[start] = -1;
    write_fd(fd, fd_id);
    write_precedence_vector(fd);
    return fd_id;
}

int create_clear_fs() {
    printf("Creating new fs file...");
    int i = 0;
    char *buf;
    FILE *pfile;
    pfile = fopen(FS_FILE_PATH, "w+");
    //allocate place for file descriptors
    buf = (char *) malloc(sizeof(fd_t));
    memset(buf, '\0', sizeof(fd_t));
    for (i = 0; i < FILE_NUMBER; i++) {
        fwrite(buf, sizeof(fd_t), 1, pfile);
    }
    free(buf);
    //allocate place for precedence vector
    buf = (char *) malloc(sizeof(int));
    memset(buf, '\0', sizeof(int));
    for (i = 0; i < BLOCK_NUMBER; i++) {
        fwrite(buf, sizeof(int), 1, pfile);
    }
    free(buf);
    //allocate place for data blocks
    buf = (char *) malloc(BLOCK_SIZE);
    memset(buf, '\0', BLOCK_SIZE);
    for (i = 0; i < BLOCK_NUMBER; i++) {
        fwrite(buf, BLOCK_SIZE, 1, pfile);
    }
    free(buf);
    fclose(pfile);
    init_fs();
    add_file("/", 1);
    printf("Done\n");
    return 0;
}

int write_data(fd_t *fd, void *data, int size, int offset) {
    int i = fd->start;
    int j = 0;
    int b = BLOCK_SIZE;
    int tempi = 0;
    if (offset == 0) {
        clear_precendence_vector(fd);
        fd->size = size;
    }
    else {
        fd->size = fd->size + size;
        do {
            tempi = i;
            i = fs.list[i];
        }
        while (i != -1);
        i = find_free_block();
        if (i == E_NFD) {
            return -1;
        }
        fs.list[tempi] = i;
        fs.list[i] = -1;
    }
    if (size == 0)
        return 0;
    FILE *pfile = fopen(FS_FILE_PATH, "r+");
    int blocks = size / b;
    blocks += size % b == 0 ? 0 : 1;
    for (j = 0; j < blocks; j++) {
        if (j > 0) {
            tempi = find_free_block();
            if (tempi == E_NFD) {
                return -1;
            }
            fs.list[i] = tempi;
            fs.list[tempi] = -1;
            i = tempi;
        }
        fseek(pfile, sizeof(fd_t) * FILE_NUMBER + sizeof(int) * BLOCK_NUMBER + i * b, SEEK_SET);
        fwrite(data + j * b, (size_t) (size - b * j > 0 ? size - b * j : b), 1, pfile);
    }
    fclose(pfile);
    write_precedence_vector(fd);
    return 0;
}


int get_data(fd_t *fd, char **buf) {
    if (fd->size == 0) {
        return 0;
    }
    FILE *pfile = fopen(FS_FILE_PATH, "r");
    char *data = NULL;
    int curr = 0;
    int i = 0;
    int size = fd->size;
    int bs = BLOCK_SIZE;
    if (fd == NULL)
        return -1;
    data = (char *) malloc((size_t) size);
    curr = fd->start;
    while (curr != -1) {
        fseek(pfile, sizeof(fd_t) * FILE_NUMBER + BLOCK_NUMBER * sizeof(int) + curr * bs, SEEK_SET);
        fread(data + i++ * bs, (size_t) (size - bs * i > 0 ? size - bs * i : bs), 1, pfile);
        curr = fs.list[curr];
    }
    *buf = data;
    fclose(pfile);
    return fd->size;
}


int get_id(char *data, int size, char *filename) {
    int i = 0;
    for (i = 0; i < size / sizeof(int); i++) {
        if (strcmp(fs.fd[((int *) data)[i]].name, filename) == 0) {
            return ((int *) data)[i];
        }
    }
    return -1;
}

fd_t *get_fd_by_id(int id) {
    return &fs.fd[id];
}

int get_fd(const char *path, fd_t **fd) {
    int size = 0;
    fd_t *meta = NULL;
    int meta_id = -1;
    int id = -1;
    char filename[FILENAME_LENGTH], *data, *start, *ptr = path;
    memset(filename, '\0', FILENAME_LENGTH);
    if (strcmp(path, "/") == 0) {
        *fd = get_fd_by_id(0);
        return 0;
    }
    if (*ptr++ == '/')
        meta = get_fd_by_id(0);
    else
        return NULL;
    while ((ptr - path) != strlen(path)) {
        if (meta->size == 0)
            return -1;
        size = get_data(meta, &data);
        meta = NULL;
        start = ptr;
        while ((*ptr++ != '/') && ((ptr - path) < strlen(path)));
        (ptr - path) < strlen(path) ? strncpy(filename, start, ptr - start - 1) : strncpy(filename, start, ptr - start);
        id = get_id(data, size, filename);
        if (id == -1)
            return -1;
        meta = get_fd_by_id(id);
        memset(filename, '\0', FILENAME_LENGTH);
        free(data);
    }
    *fd = meta;
    return id;
}


char *get_directory(char *path) {
    char *directory;
    char *p = NULL, *ptr = path;
    while (p = *ptr == '/' ? ptr : p, *ptr++ != '\0');
    if ((p - path) != 0) {
        directory = (char *) malloc(sizeof(char) * (p - path));
        strncpy(directory, path, p - path);
        directory[p - path] = '\0';
    }
    else {
        directory = (char *) malloc(sizeof(char) * 2);
        strcpy(directory, "/\0");
    }
    return directory;
}

char *get_filename(char *path) {
    char *filename;
    char *p = NULL, *ptr = path;
    while (p = *ptr == '/' ? ptr : p, *ptr++ != '\0');
    filename = (char *) malloc(sizeof(char) * (ptr - p));
    strncpy(filename, p + 1, ptr - p);
    return filename;
}

int mkfile(const char *path, int type) {
    int fd = 0, size = 0, id;
    char *directory = NULL;
    char *filename;
    int *data, *mdata;
    fd_t *directory_fd;
    directory = get_directory((char *) path);
    filename = get_filename((char *) path);
    id = get_fd(directory, &directory_fd);
    size = get_data(directory_fd, &data);
    mdata = malloc(size + sizeof(int));
    memcpy(mdata, data, (size_t) size);
    fd = add_file(filename, (char) type);
    mdata[size / sizeof(int)] = fd;
    write_data(directory_fd, mdata, (int) (size + sizeof(int)), 0);
    directory_fd->size = (int) (size + sizeof(int));
    write_fd(directory_fd, id);
    return 0;
}

int rmfile(const char *path) {
    int i = 0, j = 0;
    int fd = 0, size = 0, directory_id = -1, file_id = -1;
    char *p = NULL, *ptr = (char *) path;
    char *directory;
    int *data, *mdata;
    fd_t *directory_fd, *file_fd;
    while (p = *ptr == '/' ? ptr : p, *ptr++ != '\0');
    if ((p - path) != 0) {
        directory = (char *) malloc(sizeof(char) * (p - path));
        strncpy(directory, path, p - path);
        directory[p - path] = '\0';
    }
    else {
        directory = (char *) malloc(sizeof(char) * 2);
        strcpy(directory, "/\0");
    }
    directory_id = get_fd(directory, &directory_fd);
    file_id = get_fd(path, &file_fd);
    size = get_data(directory_fd, &data);

    mdata = (int *) (char *) malloc(size - sizeof(int));
    for (i = 0; i < size / sizeof(int); i++)
        if (data[i] != file_id)
            mdata[j++] = data[i];
    write_data(directory_fd, mdata, (int) (size - sizeof(int)), 0);
    directory_fd->size = (int) (size - sizeof(int));
    write_fd(directory_fd, directory_id);
    free(data);
    free(directory);
    return 0;
}


//------------------------------------------------------------------------//

static int fs_getattr(const char *path, struct stat *stbuf) {
    printf("$getattr...%s\n", path);
    int res = 0;

    char *data;

    fd_t *meta;
    int id = get_fd(path, &meta);
    if (id == -1)
        return -ENOENT;

    memset(stbuf, 0, sizeof(struct stat));
    if (meta->is_folder == 1) {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
    }
    else {
        stbuf->st_mode = S_IFREG | 0444;
        stbuf->st_nlink = 1;
        stbuf->st_size = meta->size;
    };
    stbuf->st_mode = stbuf->st_mode | 0777;
    return res;
}

static int fs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    printf("$create...%s\n", path);
    int res;
    res = mkfile(path, 0);
    if (res != 0)
        return -1;
    return 0;
}

static int fs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
    printf("$readdir...%s\n", path);
    int i = 0;
    (void) offset;
    (void) fi;
    char *data;
    int size;

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    fd_t *meta;
    int id = get_fd(path, &meta);

    if (id == -1)
        return -ENOENT;

    size = get_data(meta, &data);
    printf("readdir_meta_size: %d\n", size);
    if (meta->size == 0)
        return 0;

    for (i = 0; i < size / sizeof(int); i++) {
        filler(buf, fs.fd[((int *) data)[i]].name, NULL, 0);
    }
    return 0;
}

static int fs_open(const char *path, struct fuse_file_info *fi) {

    printf("$open...%s\n", path);
    fd_t *meta;
    int fd_id = 0;
    fd_id = get_fd(path, &meta);
    if (fd_id == -1)
        return -ENOENT;
    return 0;
}

static int fs_opendir(const char *path, struct fuse_file_info *fi) {
    printf("$opendir...%s\n", path);
    fd_t *meta;
    int fd_id = 0;
    fd_id = get_fd(path, &meta);
    if (fd_id == -1)
        return -ENOENT;
    return 0;
}

static int fs_mkdir(const char *path, mode_t mode) {
    printf("mkdir: %s \n", path);
    int res;
    res = mkfile(path, 1);
    //printf("mkdir res: %d\n", res);
    if (res != 0)
        return -1;
    return 0;
}

static int fs_unlink(const char *path) {
    printf("$unlink...%s\n", path);
    int res;
    res = rmfile(path);
    if (res != 0)
        return -1;
    return 0;
}

static int fs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    printf("$write...%s\n", path);
    fd_t *fd;
    int res;
    int fd_id = get_fd(path, &fd);
    if (fd_id == -1)
        return -ENOENT;
    printf("size = %d offset = %d \n", size, offset);
    res = write_data(fd, buf, size, offset);
    write_fd(fd, fd_id);
    return size;
}

static int fs_init(struct fuse_conn_info *fi) {
    printf("$init\n");
    int i = 0;
    load_fs();
    /* printf("--------------------\n");
     for (i = 0; i < FILE_NUMBER; i++)
         printf("%s %d \n", fs.fd[i].name, fs.fd[i].start);
     for (i = 0; i < BLOCK_NUMBER; i++)
         printf("%d %d \n", i, fs.list[i]);
     printf("--------------------\n");
     */

    print_fs();
}

static int fs_rmfile(const char *path) {
    printf("$rmfile\n");
    int res;
    res = rmfile(path);
    if (res != 0)
        return -1;
    return 0;
}

static int fs_read(const char *path, char *buf, size_t size, off_t offset,
                   struct fuse_file_info *fi) {
    printf("$read\n");
    size_t len;
    char *data;
    fd_t *meta;
    get_fd(path, &meta);
    if (meta == NULL)
        return -ENOENT;

    len = get_data(meta, &data);

    if (len < 0)
        return -ENOENT;
    if (offset < len) {
        if (offset + size > len) {
            size = len - offset;
        }
        memcpy(buf, data + offset, size);
    }
    else
        size = 0;
    return size;
}

static int fs_truncate(const char *path, off_t offset) {
    printf("$truncate\n");
    fd_t *fd = NULL;
    int fd_id = 0;
    if (offset == 0)
        return 0;
    fd_id = get_fd(path, &fd);
    if (fd_id == -1)
        return -1;
    truncate(fd, offset);
    write(fd, fd_id, fd->size);
    return 0;
}

static int fs_rename(const char *path, const char *new_path) {
    char *ndir = NULL, *nname = NULL;
    fd_t *ndir_fd = NULL;
    int ndir_fd_id = 0;
    int *ndir_data, *ndir_ndata;
    fd_t *fd = NULL;
    int fd_id = 0;
    ndir = get_directory(new_path);
    nname = get_filename(new_path);
    printf("%s \n %s \n", ndir, nname);
    fd_id = get_fd(path, &fd);
    strcpy(fd->name, nname);
    write_fd(fd, fd_id);
    //delete fd id from old directory
    rmfile(path);
    printf("DELETED \n");
    //write fd if to new directory

    ndir_fd_id = get_fd(ndir, &ndir_fd);
    get_data(ndir_fd, &ndir_data);

    ndir_ndata = (int *) malloc(ndir_fd->size + sizeof(int));
    ndir_ndata[ndir_fd->size / sizeof(int)] = fd_id;

    write_data(ndir_fd, ndir_ndata, ndir_fd->size + sizeof(int), 0);
    ndir_fd->size += sizeof(int);
    write_fd(ndir_fd, ndir_fd_id);

    free(ndir_data);
    free(ndir_ndata);
    return 0;
}

static int fs_release(const char *path, int flags) {
    /* Just a stub.  This method is optional and can safely be left
       unimplemented */

    (void) path;
    (void) flags;
    return 0;
}

static struct fuse_operations oper = {
        .getattr    = fs_getattr,
        .readdir    = fs_readdir,
        .open       = fs_open,
        .read       = fs_read,
        .mkdir      = fs_mkdir,
        .rmdir      = fs_rmfile,
        .create     = fs_create,
        .unlink     = fs_unlink,
        .write      = fs_write,
        .opendir    = fs_opendir,
        .init       = fs_init,
        .rename     = fs_rename,
        .truncate   = fs_truncate,
        .release    = fs_release,
};

void print_fs() {
    int i = 0;
    for (i = 0; i < FILE_NUMBER; i++) {
        printf("%d name: %s\n", i, fs.fd[i].name);
    }
}

int main(int argc, char *argv[]) {

    /*//if (access(FS_FILE_PATH, F_OK) == -1) {
    create_clear_fs();
    // mkfile("/super.txt", 0);
    //mkfile("/folder", 1);

    mkfile("/folder", 0);

    //add_file("test",0);
    fd_t *fd;//= &fs.fd[0];
    get_fd("/folder", &fd);
    char buff1[] = "Hello World\n";

    write_data(fd, buff1, 4, 0);
    printf("%d\n",sizeof(buff1));
    write_fd(fd, 2);
    write_data(fd, buff1 + 4, 4, 8);
    write_fd(fd, 2);
    write_data(fd, buff1 + 8, 4, 8);
    write_fd(fd, 2);
    write_data(fd, buff1 + 12, 1, 8);
    write_fd(fd, 2);
    char *buf = malloc(sizeof(buff1));

    get_data(fd, &buf);

    printf(buf);
    return 0;*/

    if (access(FS_FILE_PATH, F_OK) == -1) {
        create_clear_fs();
        mkfile("/super.txt", 0);
        mkfile("/folder", 1);
    }
    return fuse_main(argc, argv, &oper, NULL);
}