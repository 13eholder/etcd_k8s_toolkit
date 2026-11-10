# 提交约定

**已确立规范**（维护者 2026-11-10 决定强制执行）：

- 主题行 `type(scope): subject`：type 使用 feat / fix / docs / test / refactor / style / chore；scope 为 bench / proxy / verify 或省略；subject 用**英文**一行祈使句。
- 正文（如有）用要点列出实际变更范围，并以 `Validation:` 段落记录实际执行的命令与结果；未执行的验证明确写出，未运行的命令不得写成通过。
- 决策来源只引用仓库内被跟踪的文件；不引用 `.gitignore` 的本地资料（如 `.agents/stages/`、`ref/`）。
- 线性历史：单一 `master` 直线推进，不使用 merge 提交；发布前的历史重写须保留原提交时间戳。
