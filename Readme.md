# Backup_to_parquet_local_mysql

Este projeto realiza o backup de índices, tabelas, funções e procedimentos de um banco de dados MySQL separadamente. Além disso, exporta os dados em formato Parquet com partições, visando melhorar o desempenho de download e upload.

## Funcionalidades

- Backup separado de índices, tabelas, funções e procedimentos.
- Exportação dos dados em formato Parquet com partições para otimização de desempenho.

## Estrutura do Projeto

- `.vscode/`: Configurações do Visual Studio Code.
- `src/`: Diretório contendo o código-fonte principal.
- `.gitignore`: Arquivo especificando quais arquivos ou pastas devem ser ignorados pelo Git.
- `download.py`: Script responsável por realizar o download dos backups.
- `upload.py`: Script responsável por realizar o upload dos backups.

## Tecnologias Utilizadas

- **Python**: Linguagem principal utilizada no desenvolvimento dos scripts.

## Como Contribuir

1. Faça um fork deste repositório.
2. Crie uma branch para sua feature (`git checkout -b feature/nova-feature`).
3. Commit suas alterações (`git commit -m 'Adiciona nova feature'`).
4. Faça o push para a branch (`git push origin feature/nova-feature`).
5. Abra um Pull Request.

## Licença

Este projeto está licenciado sob a licença MIT. Veja o arquivo `LICENSE` para mais detalhes.

## Contato

Para dúvidas ou sugestões, por favor, entre em contato através do [perfil do autor no GitHub](https://github.com/rafael4g).

---

Feito com ♥ by Rafael D Silva