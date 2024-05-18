package cmd

import (
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zeromicro/go-zero/core/stores/postgres"
	"github.com/zeromicro/go-zero/tools/goctl/config"
	"github.com/zeromicro/go-zero/tools/goctl/model/sql/gen"
	"github.com/zeromicro/go-zero/tools/goctl/model/sql/model"
	"github.com/zeromicro/go-zero/tools/goctl/util/pathx"
	"os"
	"path/filepath"
	"strings"
)

var (
	outputDir string
	apiFile   string
	url       string
	schema    string
)

var rootCmd = &cobra.Command{
	Use:     "gozero-postgresql-julia",
	Short:   "针对postgresql数据库基于gozero框架生成julia的Model代码",
	Example: "gozero-postgresql-julia --dir=. --api=some.api --url=postgresql://user:password@localhost/dbname --schema=public",
	Args:    cobra.NoArgs,
	RunE:    PGJuliaGen,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().StringVar(&outputDir, "dir", ".", "生成项目目录")
	rootCmd.Flags().StringVar(&apiFile, "api", "", "API文件路径")
	rootCmd.Flags().StringVar(&url, "url", "", "PostgreSQL 连接字符串")
	rootCmd.Flags().StringVar(&schema, "schema", "public", "PostgreSQL 模式（Schema）名称")
}

func PGJuliaGen(cmd *cobra.Command, args []string) error {
	// 确保输出目录存在
	if err := pathx.MkdirIfNotExist(outputDir); err != nil {
		return err
	}

	// 配置生成器
	cfg, err := config.NewConfig("gozero")
	if err != nil {
		return err
	}

	// 调用生成函数
	return generateModels(cfg, outputDir, url, schema)
}

func generateModels(cfg *config.Config, dir, url, schema string) error {
	// 创建数据库模型生成器
	generator, err := gen.NewDefaultGenerator(dir, cfg, gen.WithPostgreSql())
	if err != nil {
		return err
	}

	// 生成 PostgreSQL 模型代码
	return fromPostgreSqlDataSource(generator, url, dir, schema)
}

func fromPostgreSqlDataSource(generator *gen.DefaultGenerator, url, dir, schema string) error {
	// 使用 PostgreSQL 生成器生成代码
	db := postgres.New(url)
	im := model.NewPostgreSqlModel(db)

	tables, err := im.GetAllTables(schema)
	if err != nil {
		return err
	}

	matchTables := make(map[string]*model.Table)
	for _, item := range tables {
		columnData, err := im.FindColumns(schema, item)
		if err != nil {
			return err
		}

		table, err := columnData.Convert()
		if err != nil {
			return err
		}

		matchTables[item] = table
	}

	if len(matchTables) == 0 {
		return errors.New("no tables matched")
	}

	// 创建 Julia 代码文件
	for _, table := range matchTables {
		juliaCode, err := generateJuliaCode(table)
		if err != nil {
			return err
		}

		juliaFilename := filepath.Join(dir, fmt.Sprintf("%s.jl", table.Name))
		err = os.WriteFile(juliaFilename, []byte(juliaCode), os.ModePerm)
		if err != nil {
			return err
		}
	}

	return nil
}

func generateJuliaCode(table *model.Table) (string, error) {
	var builder strings.Builder

	builder.WriteString(fmt.Sprintf("module %s\n\n", table.Name))

	for _, column := range table.Columns {
		juliaType := goTypeToJuliaType(column.DataType)
		builder.WriteString(fmt.Sprintf("const %s::%s\n", column.Name, juliaType))
	}

	builder.WriteString("\nend\n")

	return builder.String(), nil
}

func goTypeToJuliaType(goType string) string {
	switch goType {
	case "int", "int64":
		return "Int64"
	case "float64":
		return "Float64"
	case "string":
		return "String"
	default:
		return "Any"
	}
}
