# DCM 信息提取器

此项目用于从每个独立病例的 DICOM 目录中提取 metadata，并将每个病例的 metadata 保存为同名 CSV 文件。

目录结构

- data/                # 存放 DICOM 病例的根目录（已加入 .gitignore）
  - .gitkeep
- logs/                # 运行时日志（已加入 .gitignore）
  - .gitkeep
- src/dcm_extractor/   # 提取器源码
- requirements.txt     # Python 依赖

使用方法 (PowerShell)

1. 将每个病例放到 `data/dicom_cases/<case_name>/...` 目录下。
2. 创建虚拟环境并安装依赖：

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -r requirements.txt
```

# 运行提取

3. 基本运行提取：

```powershell
python -m src.dcm_extractor.extractor --data-root data/dicom_cases --out data/output_csv
```

4. 常用可选参数：

- `--merge-all`：在 `--out` 指定目录下生成合并文件 `all_cases.csv`（把所有 case 的 CSV 合并在一起）。
- `--export-json`：同时为每个 case 导出 JSON（`data/output_csv/<case_name>.json`）。
- `--desensitize`：在输出前对 `PatientName` 做脱敏处理（SHA-256 哈希，输出为 `hash:<16hex>` 前缀）。

新增参数与工具

- `--only-merged`：不写入单个 case 的 CSV 文件，只在指定的输出目录写入合并后的两个文件 `all_cases_original.csv` 和 `all_cases_desensitized.csv`。该选项通常与 `--merge-all` 一起使用：

```powershell
python -m src.dcm_extractor.extractor -d data/dicom_cases -o data/output_csv --merge-all --only-merged
```

- `--move-top-level-zips`：如果你把案例 zip 文件直接放在 `data/dicom_cases` 根目录（而不是每个 case 的子目录），启用该开关会把这些顶层 zip 自动移动到以 zip 名称为名的子目录中（例如 `dicom_5807164.zip` -> `data/dicom_cases/dicom_5807164/dicom_5807164.zip`），避免手动预处理。

```powershell
python -m src.dcm_extractor.extractor -d data/dicom_cases -o data/output_csv --move-top-level-zips --merge-all --only-merged
```

- `scripts/process_cases_with_timeout.py`：当单个 case 含大量 DICOM 文件或很大的 zip 时，处理时间可能较长。此脚本会为每个 case 启动一个子进程并可设置超时时间（秒），以避免某个 case 卡住整个批处理流程：

```powershell
python scripts/process_cases_with_timeout.py -d data/dicom_cases -o data/output_csv --timeout 900 --move-top-level-zips
```

参数说明：
- `--timeout`：为每个 case 指定最大处理时长（秒），默认 300 秒。超时的 case 会被终止并跳过。
- `--move-top-level-zips`：与 extractor 的同名开关行为一致，先移动 zip 再处理。

实践建议：
- 小规模测试：先在少量 case 上运行并确认输出格式无误，再对全量数据运行。对于大型 zip，可适当把 `--timeout` 调大到 900s 或更高。


示例（同时导出 JSON、生成合并表并脱敏）：

```powershell
python -m src.dcm_extractor.extractor --data-root data/dicom_cases --out data/output_csv --merge-all --export-json --desensitize
```

输出说明

- 每个病例会生成 `data/output_csv/<case_name>.csv`（现在 CSV 列采用固定模板，保证列顺序一致，便于后续汇总）。
- 如果使用 `--merge-all`，将写入 `data/output_csv/all_cases.csv`，列顺序与单个 CSV 一致。
- 如果使用 `--export-json`，将为每个 case 写入 `data/output_csv/<case_name>.json`（records 格式）。

固定列模板（CSV 列顺序）

1. FileName
2. PatientName
3. PatientID
4. StudyDate
5. PatientBirthDate
6. PatientAge
7. PatientSex
8. StudyInstanceUID
9. SeriesInstanceUID
10. Modality
11. Manufacturer
12. Rows
13. Columns
14. ImageCount
15. SeriesCount

脱敏说明

- `--desensitize` 会对 `PatientName` 应用 SHA-256，并仅保留前 16 个十六进制字符，格式为 `hash:<16hex>`，以便在保留可追溯性的同时隐藏真实姓名。

注意事项

- 本仓库把 `data/` 和 `logs/` 整个目录内容列入了 `.gitignore`，请放心把原始 DICOM 数据放入 `data/dicom_cases`。
- 当数据量大时建议使用 `--merge-all` 生成合并 CSV 后再进行下游分析，以避免逐个文件加载的开销。
 
项目内新增：ProjectID 映射与安全批处理

- `--projectid-map <path>`：传给 `extractor` 或 `scripts/process_cases_with_timeout.py` 的 JSON 文件路径，用于持久化 `case_name -> ProjectID` 的分配。首次运行会为未见过的 case 分配下一个可用整数 ID（从 1 开始），并在运行结束时保存回该 JSON 文件，以便后续重复运行时保持 ID 稳定。

  用法示例（在 timeout wrapper 中使用并保存映射）：

```powershell
python scripts/process_cases_with_timeout.py -d data/dicom_cases -o data/output_csv --timeout 300 --move-top-level-zips --projectid-map data/output_csv/case_projectid_map.json
```

- `scripts/process_cases_with_timeout.py`：推荐用于批量处理大数据集（为每个 case 使用独立子进程并设置超时，防止单个 case 卡住整个流程）。脚本现在会在运行结束时自动写入 `--projectid-map` 指定的 JSON（如果提供）。

注意：如果你更愿意直接使用主 extractor 并由它来写入映射，也可以：

```powershell
python -m src.dcm_extractor.extractor -d data/dicom_cases -o data/output_csv --merge-all --only-merged --move-top-level-zips --projectid-map data/output_csv/case_projectid_map.json
```

# DCM-information
Dicom matadata transfer
