#ifndef __DB_H
#define __DB_H

typedef struct {
  char* buffer;
  size_t buffer_length;
  size_t input_length;
} InputBuffer;

typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
  EXECUTE_SUCCESS,
  EXECUTE_TABLE_FULL,
} ExecuteResult;

typedef enum {
  PREPARE_SUCCESS,
  PREPARE_SYNTAX_ERROR,
  PREPARE_UNRECOGNIZED_STATEMENT,
} PrepareResult;

/**
 * @brief 指令类型
 */
typedef enum {
  STATEMENT_INSERT,
  STATEMENT_SELECT,
} StatementType;

// 结构体中某个字段的偏移值
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

// username 字段的最大长度
#define COLUMN_USERNAME_SIZE 32
// email字段的最大长度
#define COLUMN_EMAIL_SIZE 255
// 单表最大页数
#define TABLE_MAX_PAGES 100

typedef struct {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE];
  char email[COLUMN_EMAIL_SIZE];
} Row;

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

// 每页存储空间大小
const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = TABLE_MAX_PAGES * ROWS_PER_PAGE;

typedef struct {
  uint32_t num_rows;
  void* pages[TABLE_MAX_PAGES];
} Table;

typedef struct {
  StatementType type;
  Row row_to_insert;
} Statement;

/**
 * @brief 输出Row
 * @param row 指向该行的指针
 */
void print_row(Row* row);

/**
 * @brief 将Row结构体存入指定位置
 * @param source Row结构体
 * @param destination 目标位置
 */
void serialize_row(Row* source, void* destination);

/**
 * @brief 从存储位置构造Row结构体
 * @param source 原始存储位置
 * @param destination 目标结构体
 */
void deserialize_row(void* source, Row* destination);

/**
 * @brief 执行元指令
 * @param ib 输入字符串
 * @return 执行结果
 */
MetaCommandResult do_meta_command(InputBuffer* ib, Table* table);

/**
 * @brief 编译SQL指令
 * @param ib 输入指令
 * @param sm 指令类型
 * @return 编译成功与否
 */
PrepareResult prepare_statement(InputBuffer* ib, Statement* sm);

/**
 * @brief 计算table中特定行的内存起始位置
 * @param table 数据表
 * @param row_num 第几行
 * @return 指向该行的内存位置
 */
void* row_slot(Table* table, uint32_t row_num);

/**
 * @brief 执行插入指令
 * @param sm 指令信息
 * @param table 要插入的表格
 * @return 执行成功与否
 */
ExecuteResult execute_insert(Statement* sm, Table* table);

/**
 * @brief 执行查询指令
 * @param sm 指令信息
 * @param table 要查询的表格
 * @return 执行成功与否
 */
ExecuteResult execute_select(Statement* sm, Table* table);
/**
 * @brief 指令执行
 * @param sm
 */
ExecuteResult execute_statement(Statement* sm, Table* table);

/**
 * @brief 申请一张新Table
 * @return
 */
Table* new_table();

/**
 * @brief 释放表空间
 * @param table
 */
void free_table(Table* table);

/**
 * @brief 获取输入
 * @return 输入的数据
 */
InputBuffer* new_input_buffer();

/**
 * @brief 输出提示语
 */
void print_prompt();

/**
 * @brief 获取用户输入指令
 * @param ib 输入数据的缓冲区
 */
void read_input(InputBuffer* ib);

/**
 * @brief 释放暂存输入数据的缓冲区
 * @param ib 暂存输入数据的缓冲区
 */
void close_input_buffer(InputBuffer* ib);

#endif
