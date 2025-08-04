package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/joho/godotenv"
)

// 配置结构体
type Config struct {
	Host     string
	Port     string
	Database string
	Username string
	Password string
}

// 加载配置
func loadConfig() (*Config, error) {
	// 尝试加载.env文件
	godotenv.Load()

	config := &Config{
		Host:     getEnv("CLICKHOUSE_HOST", "localhost"),
		Port:     getEnv("CLICKHOUSE_PORT", "8123"),
		Database: getEnv("CLICKHOUSE_DATABASE", "default"),
		Username: getEnv("CLICKHOUSE_USERNAME", "default"),
		Password: getEnv("CLICKHOUSE_PASSWORD", "123456"),
	}

	return config, nil
}

// 解析后的结构体
type ParsedCoverageRecord struct {
	Hash                string
	StatementMap        map[uint32]StatementInfo
	FnMap               map[uint32]FunctionInfo
	BranchMap           map[uint32]BranchInfo
	RestoreStatementMap map[uint32]StatementInfo
	RestoreFnMap        map[uint32]FunctionInfo
	RestoreBranchMap    map[uint32]BranchInfo
	Ts                  time.Time
}

type StatementInfo struct {
	Line   uint32
	Column uint32
	Length uint32
	Count  uint32
}

type FunctionInfo struct {
	Name     string
	Line     uint32
	StartPos [4]uint32
	EndPos   [4]uint32
}

type BranchInfo struct {
	Type     uint8
	Line     uint32
	Position [4]uint32
	Paths    [][4]uint32
}

// 简单的字符串解析函数
func parseStatementMapSimple(mapStr string) map[uint32]StatementInfo {
	result := make(map[uint32]StatementInfo)
	mapStr = strings.Trim(mapStr, "{}")

	// 使用正则匹配 key:(val1,val2,val3,val4) 格式
	re := regexp.MustCompile(`(\d+):\((\d+),(\d+),(\d+),(\d+)\)`)
	matches := re.FindAllStringSubmatch(mapStr, -1)

	for _, match := range matches {
		if len(match) == 6 {
			key, _ := strconv.ParseUint(match[1], 10, 32)
			line, _ := strconv.ParseUint(match[2], 10, 32)
			column, _ := strconv.ParseUint(match[3], 10, 32)
			length, _ := strconv.ParseUint(match[4], 10, 32)
			count, _ := strconv.ParseUint(match[5], 10, 32)

			result[uint32(key)] = StatementInfo{
				Line:   uint32(line),
				Column: uint32(column),
				Length: uint32(length),
				Count:  uint32(count),
			}
		}
	}
	return result
}

func parseFunctionMapSimple(mapStr string) map[uint32]FunctionInfo {
	result := make(map[uint32]FunctionInfo)
	mapStr = strings.Trim(mapStr, "{}")

	// 匹配 key:('name',line,(pos1,pos2,pos3,pos4),(pos5,pos6,pos7,pos8))
	re := regexp.MustCompile(`(\d+):\('([^']*)',(\d+),\((\d+),(\d+),(\d+),(\d+)\),\((\d+),(\d+),(\d+),(\d+)\)\)`)
	matches := re.FindAllStringSubmatch(mapStr, -1)

	for _, match := range matches {
		if len(match) == 12 {
			key, _ := strconv.ParseUint(match[1], 10, 32)
			name := match[2]
			line, _ := strconv.ParseUint(match[3], 10, 32)

			// 解析位置信息
			startPos := [4]uint32{}
			endPos := [4]uint32{}

			for i := 0; i < 4; i++ {
				val, _ := strconv.ParseUint(match[4+i], 10, 32)
				startPos[i] = uint32(val)
			}
			for i := 0; i < 4; i++ {
				val, _ := strconv.ParseUint(match[8+i], 10, 32)
				endPos[i] = uint32(val)
			}

			result[uint32(key)] = FunctionInfo{
				Name:     name,
				Line:     uint32(line),
				StartPos: startPos,
				EndPos:   endPos,
			}
		}
	}
	return result
}

func parseBranchMapSimple(mapStr string) map[uint32]BranchInfo {
	result := make(map[uint32]BranchInfo)
	mapStr = strings.Trim(mapStr, "{}")

	// 匹配 key:(type,line,(pos1,pos2,pos3,pos4),[paths])
	re := regexp.MustCompile(`(\d+):\((\d+),(\d+),\((\d+),(\d+),(\d+),(\d+)\),\[([^\]]*)\]\)`)
	matches := re.FindAllStringSubmatch(mapStr, -1)

	for _, match := range matches {
		if len(match) == 9 {
			key, _ := strconv.ParseUint(match[1], 10, 32)
			branchType, _ := strconv.ParseUint(match[2], 10, 8)
			line, _ := strconv.ParseUint(match[3], 10, 32)

			// 解析位置
			position := [4]uint32{}
			for i := 0; i < 4; i++ {
				val, _ := strconv.ParseUint(match[4+i], 10, 32)
				position[i] = uint32(val)
			}

			// 解析路径数组
			pathsStr := match[8]
			paths := parsePaths(pathsStr)

			result[uint32(key)] = BranchInfo{
				Type:     uint8(branchType),
				Line:     uint32(line),
				Position: position,
				Paths:    paths,
			}
		}
	}
	return result
}

func parsePaths(pathsStr string) [][4]uint32 {
	var paths [][4]uint32

	// 匹配 (num1,num2,num3,num4) 格式
	re := regexp.MustCompile(`\((\d+),(\d+),(\d+),(\d+)\)`)
	matches := re.FindAllStringSubmatch(pathsStr, -1)

	for _, match := range matches {
		if len(match) == 5 {
			path := [4]uint32{}
			for i := 0; i < 4; i++ {
				val, _ := strconv.ParseUint(match[1+i], 10, 32)
				path[i] = uint32(val)
			}
			paths = append(paths, path)
		}
	}
	return paths
}

// 辅助函数：将 interface{} 转换为 uint32
func toUint32(v interface{}) uint32 {
	switch val := v.(type) {
	case uint32:
		return val
	case uint64:
		return uint32(val)
	case int:
		return uint32(val)
	case int32:
		return uint32(val)
	case int64:
		return uint32(val)
	default:
		return 0
	}
}

// 获取环境变量，如果不存在则返回默认值
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// 连接ClickHouse
func connectClickHouse(config *Config) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:     []string{fmt.Sprintf("%s:%s", config.Host, config.Port)},
		Protocol: clickhouse.HTTP,
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.Username,
			Password: config.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Debug: false,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})

	if err != nil {
		return nil, fmt.Errorf("连接ClickHouse失败: %v", err)
	}

	// 测试连接
	ctx := context.Background()
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping ClickHouse失败: %v", err)
	}

	return conn, nil
}

// 查询单行详细内容 - 使用混合方法
func querySingleRecord(conn driver.Conn) error {
	ctx := context.Background()

	fmt.Println("\n🔍 查询单行详细内容 (使用混合解析方法):")

	// 查询字符串格式的数据进行解析
	query := `
		SELECT 
			hash,
			statement_map,
			toString(fn_map) as fn_map_str,
			toString(branch_map) as branch_map_str,
			toString(restore_statement_map) as restore_statement_map_str,
			toString(restore_fn_map) as restore_fn_map_str,
			toString(restore_branch_map) as restore_branch_map_str,
			ts
		FROM coverage_map 
		LIMIT 1
	`

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("查询记录失败: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		fmt.Println("❌ 没有找到可查询的记录")
		return nil
	}

	var (
		hash, fnMapStr, branchMapStr                   string
		restoreStmtStr, restoreFnStr, restoreBranchStr string
		statementMap                                   map[uint32][]interface{}
		ts                                             time.Time
	)

	if err := rows.Scan(&hash, &statementMap, &fnMapStr, &branchMapStr,
		&restoreStmtStr, &restoreFnStr, &restoreBranchStr, &ts); err != nil {
		return fmt.Errorf("扫描记录失败: %v", err)
	}

	fmt.Printf("📋 查询Hash: %s 的详细内容\n", hash)

	// 解析各种映射
	parsedRecord := ParsedCoverageRecord{
		Hash: hash,
		Ts:   ts,
	}

	// 转换语句映射
	parsedRecord.StatementMap = make(map[uint32]StatementInfo)
	for key, stmt := range statementMap {
		if len(stmt) >= 4 {
			parsedRecord.StatementMap[key] = StatementInfo{
				Line:   toUint32(stmt[0]),
				Column: toUint32(stmt[1]),
				Length: toUint32(stmt[2]),
				Count:  toUint32(stmt[3]),
			}
		}
	}

	// 解析字符串格式的映射
	parsedRecord.FnMap = parseFunctionMapSimple(fnMapStr)
	parsedRecord.BranchMap = parseBranchMapSimple(branchMapStr)
	parsedRecord.RestoreStatementMap = parseStatementMapSimple(restoreStmtStr)
	parsedRecord.RestoreFnMap = parseFunctionMapSimple(restoreFnStr)
	parsedRecord.RestoreBranchMap = parseBranchMapSimple(restoreBranchStr)

	// 显示统计信息
	fmt.Println("\n📊 详细统计信息:")
	fmt.Printf("Hash: %s\n", parsedRecord.Hash)
	fmt.Printf("时间: %s\n", parsedRecord.Ts.Format("2006-01-02 15:04:05"))
	fmt.Printf("语句映射数量: %d\n", len(parsedRecord.StatementMap))
	fmt.Printf("函数映射数量: %d\n", len(parsedRecord.FnMap))
	fmt.Printf("分支映射数量: %d\n", len(parsedRecord.BranchMap))
	fmt.Printf("恢复语句映射数量: %d\n", len(parsedRecord.RestoreStatementMap))
	fmt.Printf("恢复函数映射数量: %d\n", len(parsedRecord.RestoreFnMap))
	fmt.Printf("恢复分支映射数量: %d\n", len(parsedRecord.RestoreBranchMap))

	totalMappings := len(parsedRecord.StatementMap) + len(parsedRecord.FnMap) + len(parsedRecord.BranchMap) +
		len(parsedRecord.RestoreStatementMap) + len(parsedRecord.RestoreFnMap) + len(parsedRecord.RestoreBranchMap)
	fmt.Printf("总映射数量: %d\n", totalMappings)

	// 显示语句映射详情
	fmt.Println("\n📋 语句映射结构:")
	if len(parsedRecord.StatementMap) > 0 {
		fmt.Println("  前5个语句映射:")
		count := 0
		for key, stmt := range parsedRecord.StatementMap {
			if count >= 5 {
				break
			}
			fmt.Printf("    Key: %d -> Line: %d, Column: %d, Length: %d, Count: %d\n",
				key, stmt.Line, stmt.Column, stmt.Length, stmt.Count)
			count++
		}
		if len(parsedRecord.StatementMap) > 5 {
			fmt.Printf("    ... 还有 %d 个语句映射\n", len(parsedRecord.StatementMap)-5)
		}
	} else {
		fmt.Println("  无语句映射数据")
	}

	// 显示函数映射详情
	fmt.Println("\n📋 函数映射结构:")
	if len(parsedRecord.FnMap) > 0 {
		fmt.Println("  前5个函数映射:")
		count := 0
		for key, fn := range parsedRecord.FnMap {
			if count >= 5 {
				break
			}
			fmt.Printf("    Key: %d -> Name: %s, Line: %d\n", key, fn.Name, fn.Line)
			fmt.Printf("      Start: (%d,%d)-(%d,%d), End: (%d,%d)-(%d,%d)\n",
				fn.StartPos[0], fn.StartPos[1], fn.StartPos[2], fn.StartPos[3],
				fn.EndPos[0], fn.EndPos[1], fn.EndPos[2], fn.EndPos[3])
			count++
		}
		if len(parsedRecord.FnMap) > 5 {
			fmt.Printf("    ... 还有 %d 个函数映射\n", len(parsedRecord.FnMap)-5)
		}
	} else {
		fmt.Println("  无函数映射数据")
	}

	// 显示分支映射详情
	fmt.Println("\n📋 分支映射结构:")
	if len(parsedRecord.BranchMap) > 0 {
		fmt.Println("  前5个分支映射:")
		count := 0
		for key, branch := range parsedRecord.BranchMap {
			if count >= 5 {
				break
			}
			fmt.Printf("    Key: %d -> Type: %d, Line: %d, Position: (%d,%d)-(%d,%d), Paths: %d\n",
				key, branch.Type, branch.Line,
				branch.Position[0], branch.Position[1], branch.Position[2], branch.Position[3],
				len(branch.Paths))

			// 显示前3个分支路径
			for i, path := range branch.Paths {
				if i >= 3 {
					fmt.Printf("      ... 还有 %d 个路径\n", len(branch.Paths)-3)
					break
				}
				fmt.Printf("      Path %d: (%d,%d)-(%d,%d)\n",
					i, path[0], path[1], path[2], path[3])
			}
			count++
		}
		if len(parsedRecord.BranchMap) > 5 {
			fmt.Printf("    ... 还有 %d 个分支映射\n", len(parsedRecord.BranchMap)-5)
		}
	} else {
		fmt.Println("  无分支映射数据")
	}

	// 使用示例
	fmt.Println("\n💡 使用示例:")
	if len(parsedRecord.StatementMap) > 0 {
		// 获取第一个语句映射
		for key, stmt := range parsedRecord.StatementMap {
			fmt.Printf("第一个语句 (Key: %d): 第%d行第%d列，长度%d，计数%d\n",
				key, stmt.Line, stmt.Column, stmt.Length, stmt.Count)
			break
		}
	}

	if len(parsedRecord.FnMap) > 0 {
		// 获取第一个函数映射
		for key, fn := range parsedRecord.FnMap {
			fmt.Printf("第一个函数 (Key: %d): %s，第%d行\n", key, fn.Name, fn.Line)
			break
		}
	}

	if len(parsedRecord.BranchMap) > 0 {
		// 获取第一个分支映射
		for key, branch := range parsedRecord.BranchMap {
			fmt.Printf("第一个分支 (Key: %d): 类型%d，第%d行，路径数%d\n",
				key, branch.Type, branch.Line, len(branch.Paths))
			break
		}
	}

	return nil
}

func main() {
	log.Println("🚀 启动 ClickHouse Go 客户端...")

	// 加载配置
	config, err := loadConfig()
	if err != nil {
		log.Fatalf("❌ 加载配置失败: %v", err)
	}

	log.Printf("📡 连接到 ClickHouse: %s:%s", config.Host, config.Port)

	// 连接ClickHouse
	conn, err := connectClickHouse(config)
	if err != nil {
		log.Fatalf("❌ %v", err)
	}
	defer conn.Close()

	log.Println("✅ ClickHouse 连接成功!")

	// 查询单行详细内容
	if err := querySingleRecord(conn); err != nil {
		log.Printf("❌ 查询单行详细内容失败: %v", err)
	}

	log.Println("\n🎉 程序执行完成!")
}
