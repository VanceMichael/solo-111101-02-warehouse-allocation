# 大促多渠道库存采集对账

本项目用于本地采集记录的结构化校验与 SQLite 保存。采集输入采用包含 `source`、`observed_at` 和 `payload` 的 JSON 对象；记录指纹基于规范化内容生成。目录分为记录解析、存储和命令行入口，测试只使用临时目录和合成数据。

## 环境

Python 3.11 或更高版本，无第三方运行依赖。

## 测试与构建检查

```sh
python3 -m unittest discover -s tests -v
python3 -m compileall -q collector_02 tests
```

## 初始化本地记录库

```sh
python3 -m collector_02.cli --database ./data.sqlite3 --init
```
