#!/usr/bin/env python3
"""
Script ETL para importação dos dados de preços de medicamentos
do arquivo CSV para o banco de dados relacional PostgreSQL

Características:
- Processamento por savepoints: cada linha processada em savepoint isolado
- Mapeamento de IDs: sistema de obter_ou_criar_id para garantir referências corretas
- Normalização de Enums: conversão automática de valores CSV para tipos ENUM
- Validação de dados: verificação de campos obrigatórios antes da inserção
- Logging detalhado: registro de sucessos e falhas por linha processada

Uso:
    python3 etl/import_data.py --host localhost --database medicamentos_gov \\
        --user postgres --password admin --csv TA_PRECO_MEDICAMENTO_GOV.csv --skip 72
"""

import os
import csv
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import sys
from decimal import Decimal, InvalidOperation


class MedicamentosETL:
    """Classe para realizar o processo ETL dos dados de medicamentos"""

    # Mapeamento de nomes de tabelas para nomes de colunas ID
    ID_COLUMN_MAP = {
        "classes_terapeuticas": "id_classe",
        "substancias": "id_substancia",
        "laboratorios": "id_laboratorio",
        "tipos_produto": "id_tipo",
        "regimes_preco": "id_regime",
    }

    def __init__(self, host, database, user, password, csv_file):
        """
        Inicializa conexão com banco de dados PostgreSQL e arquivo CSV

        Args:
            host: Host do banco de dados
            database: Nome do banco de dados
            user: Usuário do banco
            password: Senha do banco
            csv_file: Caminho para arquivo CSV
        """
        self.csv_file = csv_file
        self.connection = None
        self.cursor = None

        try:
            conn_kwargs = {
                "host": host,
                "database": database,
                "user": user,
            }
            # Permite conexão sem senha local (peer/.pgpass). Só envia a senha se fornecida.
            if password:
                conn_kwargs["password"] = password
            self.connection = psycopg2.connect(**conn_kwargs)
            # Usa autocommit para evitar problemas de transação abortada
            self.connection.autocommit = False
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            print(f"✓ Conectado ao banco de dados {database}")
        except Exception as e:
            print(f"✗ Erro ao conectar ao banco: {e}")
            sys.exit(1)

    def limpar_valor_numerico(self, valor):
        """Converte string numérica para Decimal, tratando vírgulas e valores vazios"""
        if (
            not valor
            or valor.strip() == ""
            or valor.strip() == "-"
            or valor.strip() == "    -     "
        ):
            return None

        # Remove espaços e substitui vírgula por ponto
        valor_limpo = valor.strip().replace(",", ".")

        try:
            return Decimal(valor_limpo)
        except (InvalidOperation, ValueError):
            return None

    def _extrair_id_do_resultado(self, resultado, nome_id):
        """Extrai ID de um resultado de query de forma segura"""
        if not resultado:
            return None
        if isinstance(resultado, dict):
            return resultado.get("id") or resultado.get(nome_id)
        elif hasattr(resultado, "id"):
            return resultado.id
        return None

    def _obter_campo_csv(self, linha, indice, default=""):
        """Obtém campo do CSV de forma segura"""
        return linha[indice].strip() if len(linha) > indice else default

    def _normalizar_enum_sim_nao(self, valor):
        """Normaliza valor para enum tipo_sim_nao"""
        if not valor:
            return "Não"
        valor_upper = valor.upper()
        if valor_upper == "SIM" or "CONFAZ" in valor_upper:
            return "Sim"
        return "Não"

    def _normalizar_enum_restricao(self, valor):
        """Normaliza valor para enum tipo_restricao"""
        if not valor:
            return "Não especificado"
        valor_upper = valor.upper()
        if valor_upper == "SIM":
            return "Sim"
        elif valor_upper == "NÃO" or valor_upper == "NAO":
            return "Não"
        return "Não especificado"

    def obter_ou_criar_id(self, tabela, campo, valor, campos_extra=None):
        """
        Obtém ID de um registro ou cria se não existir

        Args:
            tabela: Nome da tabela
            campo: Nome do campo para buscar
            valor: Valor a buscar
            campos_extra: Dict com campos adicionais para inserção
        """
        if not valor or valor.strip() == "":
            return None

        # Extrai nome do ID da tabela usando mapeamento ou padrão
        if tabela in self.ID_COLUMN_MAP:
            nome_id = self.ID_COLUMN_MAP[tabela]
        else:
            # Padrão: remove 's' final e adiciona 'id_'
            nome_id = f"id_{tabela.rstrip('s')}"

        try:
            # Busca registro existente
            query = f"SELECT {nome_id} as id FROM {tabela} WHERE {campo} = %s"
            self.cursor.execute(query, (valor,))
            resultado = self.cursor.fetchone()

            id_value = self._extrair_id_do_resultado(resultado, nome_id)
            if id_value is not None:
                return id_value
            if resultado:
                raise Exception(f"Resultado não contém 'id' ou '{nome_id}'. Chaves disponíveis: {list(resultado.keys()) if isinstance(resultado, dict) else 'N/A'}")

            # Cria novo registro
            campos = [campo]
            valores = [valor]

            if campos_extra:
                campos.extend(campos_extra.keys())
                valores.extend(campos_extra.values())

            placeholders = ", ".join(["%s"] * len(campos))
            campos_str = ", ".join(campos)

            insert_query = f"INSERT INTO {tabela} ({campos_str}) VALUES ({placeholders}) RETURNING {nome_id}"
            self.cursor.execute(insert_query, valores)
            resultado_insert = self.cursor.fetchone()
            
            id_value = self._extrair_id_do_resultado(resultado_insert, nome_id)
            if id_value is not None:
                return id_value
            
            # Se não retornou ID, tenta buscar novamente (pode ter sido inserido por outra transação)
            self.cursor.execute(query, (valor,))
            resultado = self.cursor.fetchone()
            id_value = self._extrair_id_do_resultado(resultado, nome_id)
            if id_value is not None:
                return id_value
            
            raise Exception(f"Falha ao obter ID após inserção em {tabela}. INSERT não retornou resultado e busca subsequente também falhou.")
                
        except Exception as e:
            # Re-raise com contexto adicional
            raise Exception(f"Erro em obter_ou_criar_id({tabela}, {campo}, {valor[:50] if valor else None}): {e}")
    
    def _recriar_cursor_se_necessario(self):
        """Recria cursor se estiver em estado inválido"""
        try:
            self.cursor.execute("SELECT 1")
        except Exception:
            try:
                self.cursor.close()
            except Exception:
                pass
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)

    def _upsert_preco(self, tabela, campo_preco, valor, id_produto, data_vigencia):
        """Faz upsert de preço (UPDATE ou INSERT)"""
        if valor is None:
            return
        
        campo_com_impostos = campo_preco.replace("_sem_impostos", "_com_impostos")
        
        # UPDATE primeiro
        self.cursor.execute(
            f"""
            UPDATE {tabela}
            SET {campo_preco} = %s
            WHERE id_produto = %s AND id_aliquota IS NULL AND data_vigencia = %s
            """,
            (float(valor), id_produto, data_vigencia),
        )
        
        # INSERT se não atualizou
        if self.cursor.rowcount == 0:
            self.cursor.execute(
                f"""
                INSERT INTO {tabela}
                    (id_produto, id_aliquota, {campo_preco}, {campo_com_impostos}, data_vigencia)
                VALUES (%s, NULL, %s, NULL, %s)
                """,
                (id_produto, float(valor), data_vigencia),
            )

    def processar_linha_csv(self, linha, linha_num, savepoint_name=None):
        """Processa uma linha do CSV e insere no banco de dados"""
        try:
            # Campos principais
            substancia = self._obter_campo_csv(linha, 0)
            cnpj = self._obter_campo_csv(linha, 1)
            laboratorio = self._obter_campo_csv(linha, 2)
            codigo_ggrem = self._obter_campo_csv(linha, 3)
            registro = self._obter_campo_csv(linha, 4)
            ean1 = self._obter_campo_csv(linha, 5)
            ean2 = self._obter_campo_csv(linha, 6)
            ean3 = self._obter_campo_csv(linha, 7)
            produto = self._obter_campo_csv(linha, 8)
            apresentacao = self._obter_campo_csv(linha, 9)
            classe_terapeutica = self._obter_campo_csv(linha, 10)
            tipo_produto = self._obter_campo_csv(linha, 11)
            regime_preco = self._obter_campo_csv(linha, 12)

            # Validações básicas
            if not substancia or not codigo_ggrem or not produto:
                return False

            # Processa entidades relacionais
            id_substancia = self.obter_ou_criar_id(
                "substancias", "nome_substancia", substancia
            )
            id_laboratorio = self.obter_ou_criar_id(
                "laboratorios", "cnpj", cnpj, {"nome_laboratorio": laboratorio}
            )

            # Classe terapêutica pode ter código e descrição separados
            partes_classe = classe_terapeutica.split(" - ", 1)
            codigo_classe = partes_classe[0] if partes_classe else classe_terapeutica
            descricao_classe = (
                partes_classe[1] if len(partes_classe) > 1 else classe_terapeutica
            )

            id_classe = self.obter_ou_criar_id(
                "classes_terapeuticas",
                "codigo_classe",
                codigo_classe,
                {"descricao_classe": descricao_classe},
            )

            id_tipo = self.obter_ou_criar_id(
                "tipos_produto", "tipo_produto", tipo_produto
            )
            id_regime = self.obter_ou_criar_id(
                "regimes_preco", "regime_preco", regime_preco
            )

            # Campos adicionais do produto
            campos_adicionais_idx = 65
            restricao_hospitalar = self._normalizar_enum_restricao(
                self._obter_campo_csv(linha, campos_adicionais_idx, "Não")
            )
            cap = self._normalizar_enum_sim_nao(
                self._obter_campo_csv(linha, campos_adicionais_idx + 1, "Não")
            )
            confaz87 = self._normalizar_enum_sim_nao(
                self._obter_campo_csv(linha, campos_adicionais_idx + 2, "Não")
            )
            icms_zero = self._normalizar_enum_sim_nao(
                self._obter_campo_csv(linha, campos_adicionais_idx + 3, "Não")
            )
            analise_recursal = self._obter_campo_csv(linha, campos_adicionais_idx + 4)
            lista_credito = self._obter_campo_csv(linha, campos_adicionais_idx + 5)
            comercializacao = self._normalizar_enum_sim_nao(
                self._obter_campo_csv(linha, campos_adicionais_idx + 6, "Não")
            )
            tarja = self._obter_campo_csv(linha, campos_adicionais_idx + 7)

            # Verifica se produto já existe
            self.cursor.execute(
                "SELECT id_produto FROM produtos WHERE codigo_ggrem = %s",
                (codigo_ggrem,),
            )
            produto_existente = self.cursor.fetchone()
            id_produto_existente = self._extrair_id_do_resultado(produto_existente, "id_produto")

            if id_produto_existente:
                # Atualiza produto existente
                query_produto = """
                    UPDATE produtos SET
                        registro = %s, ean_1 = %s, ean_2 = %s, ean_3 = %s, nome_produto = %s, apresentacao = %s,
                        id_substancia = %s, id_laboratorio = %s, id_classe = %s, id_tipo = %s, id_regime = %s,
                        restricao_hospitalar = %s::tipo_restricao, cap = %s::tipo_sim_nao, confaz_87 = %s::tipo_sim_nao, 
                        icms_zero = %s::tipo_sim_nao, analise_recursal = %s,
                        lista_concessao_credito = %s, comercializacao_2024 = %s::tipo_sim_nao, tarja = %s
                    WHERE codigo_ggrem = %s
                """
                valores_produto = (
                    registro or None,
                    ean1 or None,
                    ean2 or None,
                    ean3 or None,
                    produto,
                    apresentacao,
                    id_substancia,
                    id_laboratorio,
                    id_classe,
                    id_tipo,
                    id_regime,
                    restricao_hospitalar,
                    cap,
                    confaz87,
                    icms_zero,
                    analise_recursal or None,
                    lista_credito or None,
                    comercializacao,
                    tarja or None,
                    codigo_ggrem,
                )
                self.cursor.execute(query_produto, valores_produto)
                id_produto = id_produto_existente
            else:
                # Insere novo produto
                query_produto = """
                    INSERT INTO produtos (
                        codigo_ggrem, registro, ean_1, ean_2, ean_3, nome_produto, apresentacao,
                        id_substancia, id_laboratorio, id_classe, id_tipo, id_regime,
                        restricao_hospitalar, cap, confaz_87, icms_zero, analise_recursal,
                        lista_concessao_credito, comercializacao_2024, tarja
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::tipo_restricao, %s::tipo_sim_nao, %s::tipo_sim_nao, %s::tipo_sim_nao, %s, %s, %s::tipo_sim_nao, %s)
                    RETURNING id_produto
                """
                valores_produto = (
                    codigo_ggrem,
                    registro or None,
                    ean1 or None,
                    ean2 or None,
                    ean3 or None,
                    produto,
                    apresentacao,
                    id_substancia,
                    id_laboratorio,
                    id_classe,
                    id_tipo,
                    id_regime,
                    restricao_hospitalar,
                    cap,
                    confaz87,
                    icms_zero,
                    analise_recursal or None,
                    lista_credito or None,
                    comercializacao,
                    tarja or None,
                )
                self.cursor.execute(query_produto, valores_produto)
                resultado_produto = self.cursor.fetchone()
                id_produto = self._extrair_id_do_resultado(resultado_produto, "id_produto")
                if id_produto is None:
                    raise Exception("INSERT de produto não retornou id_produto")

            if not id_produto:
                return False

            # Preços SEM IMPOSTOS apenas (regra de negócio simplificada)
            # Índices do CSV: 13 = PF sem impostos, 39 = PMVG sem impostos
            pf_sem_impostos = (
                self.limpar_valor_numerico(linha[13]) if len(linha) > 13 else None
            )
            pmvg_sem_impostos = (
                self.limpar_valor_numerico(linha[39]) if len(linha) > 39 else None
            )

            hoje = datetime.now().date()

            # Upsert para preços (ON CONFLICT não funciona bem com NULL, então usa UPDATE + INSERT)
            self._upsert_preco("precos_fabrica", "pf_sem_impostos", pf_sem_impostos, id_produto, hoje)
            self._upsert_preco("precos_pmvg", "pmvg_sem_impostos", pmvg_sem_impostos, id_produto, hoje)

            return True

        except Exception as e:
            # Propaga o erro para ser tratado no nível superior com savepoint
            raise

    def executar_etl(self, pular_linhas=72):
        """Executa o processo completo de ETL"""
        print(f"\nIniciando processo ETL do arquivo: {self.csv_file}")
        print(f"Pulando {pular_linhas} linhas de cabeçalho...\n")

        # Não é necessário pré-processar alíquotas quando usamos preços sem impostos
        self.connection.commit()

        linhas_processadas = 0
        linhas_sucesso = 0
        linhas_erro = 0

        try:
            with open(self.csv_file, "r", encoding="utf-8", errors="ignore") as arquivo:
                leitor = csv.reader(arquivo, delimiter=";")

                # Pula linhas de cabeçalho
                for _ in range(pular_linhas):
                    next(leitor, None)

                for linha_num, linha in enumerate(leitor, start=pular_linhas + 1):
                    if len(linha) < 10:
                        continue

                    linhas_processadas += 1
                    
                    # Usa savepoint para isolar erros por linha
                    savepoint_name = f"sp_line_{linha_num}"

                    try:
                        # Cria savepoint antes de processar a linha
                        self.cursor.execute(f"SAVEPOINT {savepoint_name}")
                        
                        if self.processar_linha_csv(linha, linha_num, savepoint_name):
                            # Commit imediatamente após sucesso para evitar problemas de transação
                            self.connection.commit()
                            linhas_sucesso += 1
                        else:
                            # Reverte savepoint em caso de erro
                            try:
                                self.cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                                self.connection.commit()  # Commit do rollback
                            except Exception:
                                self.connection.rollback()
                            linhas_erro += 1
                    except Exception as row_error:
                        # Erro ao criar savepoint ou processar - faz rollback completo e recria cursor se necessário
                        try:
                            self.connection.rollback()
                            self._recriar_cursor_se_necessario()
                        except Exception:
                            pass
                        linhas_erro += 1
                        print(f"✗ Erro ao processar linha {linha_num}: {row_error}")

                    # Progress report a cada 100 linhas
                    if linhas_processadas % 100 == 0:
                        print(
                            f"Processadas {linhas_processadas} linhas... (Sucesso: {linhas_sucesso}, Erro: {linhas_erro})"
                        )

                # Commit final
                self.connection.commit()

        except Exception as e:
            print(f"\n✗ Erro durante processamento: {e}")
            import traceback

            traceback.print_exc()
            self.connection.rollback()
            raise

        print("\n✓ Processo ETL concluído!")
        print(f"  Total de linhas processadas: {linhas_processadas}")
        print(f"  Linhas com sucesso: {linhas_sucesso}")
        print(f"  Linhas com erro: {linhas_erro}")

    def fechar(self):
        """Fecha conexão com banco de dados"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("✓ Conexão fechada")


def main():
    """Função principal"""
    import argparse

    parser = argparse.ArgumentParser(
        description="ETL para importação de dados de medicamentos (PostgreSQL)"
    )
    parser.add_argument(
        "--host",
        default=os.getenv("PGHOST", "localhost"),
        help="Host do banco de dados",
    )
    parser.add_argument(
        "--database",
        default=os.getenv("PGDATABASE", "medicamentos_gov"),
        help="Nome do banco de dados",
    )
    parser.add_argument(
        "--user",
        default=os.getenv("PGUSER", "postgres"),
        help="Usuário do banco de dados",
    )
    parser.add_argument(
        "--password",
        default=os.getenv("PGPASSWORD"),
        help="Senha do banco de dados (opcional, usa .pgpass/peer se ausente)",
    )
    parser.add_argument(
        "--csv",
        default="TA_PRECO_MEDICAMENTO_GOV.csv",
        help="Arquivo CSV para importar",
    )
    parser.add_argument(
        "--skip", type=int, default=72, help="Número de linhas a pular (cabeçalho)"
    )

    args = parser.parse_args()

    etl = MedicamentosETL(args.host, args.database, args.user, args.password, args.csv)

    try:
        etl.executar_etl(pular_linhas=args.skip)
    finally:
        etl.fechar()


if __name__ == "__main__":
    main()
