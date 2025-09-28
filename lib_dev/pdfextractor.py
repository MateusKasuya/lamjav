import os
from typing import Optional, List, Dict, Any

import camelot
import pandas as pd


class PDFTableExtractor:
    """Classe para extrair dados de tabelas de arquivos PDF."""

    def __init__(self, file_name: str, configs: Optional[Dict[str, Any]] = None):
        """
        Inicializa o extrator de tabelas PDF.

        Args:
            file_name: Caminho para o arquivo PDF
            configs: Configurações opcionais para extração
        """
        self.file_name = file_name
        self.configs = configs or {}

        # Verifica se o arquivo existe
        if not os.path.exists(file_name):
            raise FileNotFoundError(f"Arquivo não encontrado: {file_name}")

    @staticmethod
    def fix_header(df: pd.DataFrame) -> pd.DataFrame:
        """
        Corrige o cabeçalho do DataFrame usando a primeira linha como header.

        Args:
            df: DataFrame a ser corrigido

        Returns:
            DataFrame com cabeçalho corrigido
        """
        if df.empty:
            return df

        # Cria uma cópia para não modificar o original
        df_copy = df.copy()

        # Usa a primeira linha como cabeçalho
        new_columns = df_copy.iloc[0].astype(str).tolist()
        df_copy.columns = new_columns

        # Remove apenas a primeira linha (que agora é o cabeçalho)
        df_copy = df_copy.iloc[1:].reset_index(drop=True)

        return df_copy

    def get_table_data(
        self,
        table_columns: Optional[List[str]] = None,
        fix: bool = True,
        table_areas: Optional[List[str]] = None,
        columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Extrai dados de tabela do PDF.

        Args:
            table_columns: Nomes das colunas esperadas
            fix: Se deve corrigir o cabeçalho
            table_areas: Áreas específicas da tabela
            columns: Posições das colunas

        Returns:
            DataFrame com os dados extraídos
        """
        try:
            # Usar detecção automática para capturar mais dados
            all_tables = []

            # Tentar extrair de todas as páginas com detecção automática
            for page_num in range(1, 10):  # Tentar até 10 páginas
                try:
                    page_tables = camelot.read_pdf(
                        self.file_name,
                        flavor="stream",
                        pages=str(page_num),
                        strip_text="\n",
                    )

                    if page_tables:
                        all_tables.extend(page_tables)

                except Exception as e:
                    # Se erro indica que a página não existe, parar
                    if "Invalid page" in str(e) or "does not exist" in str(e):
                        break
                    else:
                        continue

            if not all_tables:
                raise ValueError("Nenhuma tabela encontrada em qualquer página")

            tables = all_tables

            if not tables:
                raise ValueError("Nenhuma tabela encontrada no PDF")

            # Processa cada tabela mantendo a ordem das páginas
            table_content = []
            page_1_headers = None  # Guardar headers da página 1

            for i, table in enumerate(tables):
                page_num = i + 1

                # Processa o DataFrame
                processed_df = table.df.copy()

                if fix and page_num == 1:
                    # Apenas a página 1 define os headers
                    processed_df = self.fix_header(processed_df)

                    # Remover primeira coluna se estiver vazia (índice 0)
                    if (
                        len(processed_df.columns) > 0
                        and processed_df.iloc[:, 0]
                        .fillna("")
                        .astype(str)
                        .str.strip()
                        .eq("")
                        .all()
                    ):
                        processed_df = processed_df.iloc[:, 1:]

                    # Filtrar apenas as colunas que queremos (excluindo Reason)
                    desired_columns = [
                        "Game Date",
                        "Game Time",
                        "Matchup",
                        "Team",
                        "Player Name",
                        "Current Status",
                    ]
                    available_columns = processed_df.columns.tolist()

                    # Mapear colunas disponíveis para as desejadas
                    column_mapping = {}
                    for desired in desired_columns:
                        for available in available_columns:
                            if desired.lower() in str(available).lower():
                                column_mapping[available] = desired
                                break

                    # Renomear colunas encontradas
                    processed_df = processed_df.rename(columns=column_mapping)

                    # Selecionar apenas as colunas desejadas (se existirem)
                    final_columns = []
                    for col in desired_columns:
                        if col in processed_df.columns:
                            final_columns.append(col)
                        else:
                            # Adicionar coluna vazia se não encontrada
                            processed_df[col] = ""
                            final_columns.append(col)

                    processed_df = processed_df[final_columns]
                    page_1_headers = final_columns

                elif fix and page_num > 1:
                    # Páginas 2+ - processar de forma mais flexível

                    # Remover primeira coluna se estiver vazia (índice 0)
                    if (
                        len(processed_df.columns) > 0
                        and processed_df.iloc[:, 0]
                        .fillna("")
                        .astype(str)
                        .str.strip()
                        .eq("")
                        .all()
                    ):
                        processed_df = processed_df.iloc[:, 1:]

                    # Para páginas 2+, tentar identificar jogadores diretamente
                    if page_1_headers:
                        # Tentar mapear colunas usando o mesmo padrão da página 1
                        desired_columns = [
                            "Game Date",
                            "Game Time",
                            "Matchup",
                            "Team",
                            "Player Name",
                            "Current Status",
                        ]
                        available_columns = processed_df.columns.tolist()

                        column_mapping = {}
                        for desired in desired_columns:
                            for available in available_columns:
                                if desired.lower() in str(available).lower():
                                    column_mapping[available] = desired
                                    break

                        # Renomear colunas encontradas
                        processed_df = processed_df.rename(columns=column_mapping)

                        # Selecionar apenas as colunas desejadas (se existirem)
                        final_columns = []
                        for col in desired_columns:
                            if col in processed_df.columns:
                                final_columns.append(col)
                            else:
                                # Adicionar coluna vazia se não encontrada
                                processed_df[col] = ""
                                final_columns.append(col)

                        processed_df = processed_df[final_columns]
                        processed_df.columns = page_1_headers
                    else:
                        # Se não temos headers da página 1, usar processamento básico
                        processed_df = self._process_page_without_headers(
                            processed_df, page_num
                        )
                else:
                    # Remover primeira coluna se estiver vazia (índice 0)
                    if (
                        len(processed_df.columns) > 0
                        and processed_df.iloc[:, 0]
                        .fillna("")
                        .astype(str)
                        .str.strip()
                        .eq("")
                        .all()
                    ):
                        processed_df = processed_df.iloc[:, 1:]

                # Garantir que o DataFrame tenha pelo menos uma linha
                if processed_df.empty:
                    continue

                # Adiciona coluna de metadados da página
                processed_df["_page_number"] = page_num

                table_content.append(processed_df)

            # Verifica se temos dados para combinar
            if not table_content:
                raise ValueError("Nenhuma tabela válida encontrada em qualquer página")

            # Combina múltiplas tabelas preservando a ordem
            if len(table_content) > 1:
                # Garantir que todas as tabelas tenham as mesmas colunas
                all_columns = set()
                for df in table_content:
                    all_columns.update(df.columns)

                # Padronizar colunas em todos os DataFrames
                standardized_dfs = []
                for df in table_content:
                    df_standardized = df.copy()
                    for col in all_columns:
                        if col not in df_standardized.columns:
                            df_standardized[col] = (
                                ""  # Adiciona coluna faltante com valor vazio
                            )

                    # Reordenar colunas na mesma ordem
                    df_standardized = df_standardized[sorted(all_columns)]
                    standardized_dfs.append(df_standardized)

                result = pd.concat(standardized_dfs, ignore_index=True)
            else:
                result = table_content[0]

            # Consolidar linhas relacionadas primeiro (versão melhorada)
            result = self._consolidate_related_rows_improved(result)

            # Depois limpar dados fragmentados/órfãos
            result = self._clean_fragmented_data_improved(result)

            return result

        except Exception as e:
            raise RuntimeError(f"Erro ao extrair tabela do PDF: {str(e)}")

    def _process_page_without_headers(
        self, df: pd.DataFrame, page_num: int
    ) -> pd.DataFrame:
        """
        Processa uma página sem headers definidos, tentando identificar jogadores.

        Args:
            df: DataFrame da página
            page_num: Número da página

        Returns:
            DataFrame processado
        """
        if df.empty:
            return df

        # Criar DataFrame com estrutura padrão
        result_df = pd.DataFrame(
            columns=[
                "Game Date",
                "Game Time",
                "Matchup",
                "Team",
                "Player Name",
                "Current Status",
            ]
        )

        # Procurar por jogadores na página
        players_found = []

        for idx, row in df.iterrows():
            row_values = [str(cell).strip() for cell in row]

            # Procurar por padrão de nome de jogador (Sobrenome, Nome)
            for i, value in enumerate(row_values):
                if "," in value and len(value.split(",")) == 2:
                    parts = value.split(",")
                    if len(parts[0].strip()) > 2 and len(parts[1].strip()) > 1:
                        # Encontrou um jogador
                        player_name = value

                        # Tentar encontrar status na mesma linha ou próxima
                        status = ""
                        team = ""
                        game_date = ""
                        game_time = ""
                        matchup = ""

                        # Procurar status na mesma linha
                        for j, other_value in enumerate(row_values):
                            if other_value in [
                                "Out",
                                "Available",
                                "Questionable",
                                "Probable",
                                "Doubtful",
                            ]:
                                status = other_value
                                break

                        # Se não encontrou status na mesma linha, verificar próxima linha
                        if not status and idx + 1 < len(df):
                            next_row_values = [
                                str(cell).strip() for cell in df.iloc[idx + 1]
                            ]
                            for other_value in next_row_values:
                                if other_value in [
                                    "Out",
                                    "Available",
                                    "Questionable",
                                    "Probable",
                                    "Doubtful",
                                ]:
                                    status = other_value
                                    break

                        # Procurar por times (padrão: termina com palavras como "Kings", "Pistons", etc.)
                        for j, other_value in enumerate(row_values):
                            if any(
                                team_word in other_value
                                for team_word in [
                                    "Kings",
                                    "Pistons",
                                    "Heat",
                                    "76ers",
                                    "Hawks",
                                    "Magic",
                                    "Bulls",
                                    "Cavaliers",
                                    "Grizzlies",
                                    "Hornets",
                                    "Wizards",
                                    "Pacers",
                                    "Celtics",
                                    "Knicks",
                                    "Pelicans",
                                    "Nets",
                                    "Lakers",
                                    "Thunder",
                                    "Timberwolves",
                                    "Bucks",
                                    "Warriors",
                                    "Suns",
                                    "Spurs",
                                    "Clippers",
                                ]
                            ):
                                team = other_value
                                break

                        # Procurar por datas (padrão: MM/DD/YYYY)
                        for j, other_value in enumerate(row_values):
                            if "/" in other_value and len(other_value.split("/")) == 3:
                                game_date = other_value
                                break

                        # Procurar por horários (padrão: HH:MM (ET))
                        for j, other_value in enumerate(row_values):
                            if "(ET)" in other_value and ":" in other_value:
                                game_time = other_value
                                break

                        # Procurar por matchups (padrão: AAA@BBB)
                        for j, other_value in enumerate(row_values):
                            if "@" in other_value and len(other_value.split("@")) == 2:
                                matchup = other_value
                                break

                        players_found.append(
                            {
                                "Game Date": game_date,
                                "Game Time": game_time,
                                "Matchup": matchup,
                                "Team": team,
                                "Player Name": player_name,
                                "Current Status": status,
                            }
                        )

        if players_found:
            result_df = pd.DataFrame(players_found)

        return result_df

    def get_table_data_from_page(
        self,
        page_number: int,
        table_areas: List[str],
        table_columns: Optional[List[str]] = None,
        fix: bool = True,
    ) -> pd.DataFrame:
        """
        Extrai dados de tabela de uma página específica.

        Args:
            page_number: Número da página
            table_areas: Áreas da tabela na página
            table_columns: Nomes das colunas esperadas
            fix: Se deve corrigir o cabeçalho

        Returns:
            DataFrame com os dados da página
        """
        try:
            tables = camelot.read_pdf(
                self.file_name,
                pages=str(page_number),
                flavor="stream",
                table_areas=table_areas,
                strip_text="\n",
            )

            if not tables:
                raise ValueError(f"Nenhuma tabela encontrada na página {page_number}")

            # Processa a primeira tabela encontrada
            df = tables[0].df.copy()

            if fix:
                df = self.fix_header(df)

            # Adiciona metadados da página
            df["_page_number"] = page_number

            return df

        except Exception as e:
            raise RuntimeError(
                f"Erro ao extrair tabela da página {page_number}: {str(e)}"
            )

    def sanitize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Sanitiza os nomes das colunas do DataFrame.

        Args:
            df: DataFrame com colunas a serem sanitizadas

        Returns:
            DataFrame com nomes de colunas sanitizados
        """
        if df.empty:
            return df

        df_copy = df.copy()

        # Preserva colunas de metadados (que começam com _)
        [col for col in df_copy.columns if str(col).startswith("_")]

        # Converte para string e limpa os nomes
        new_columns = []
        for col in df_copy.columns:
            col_str = str(col)
            if col_str.startswith("_"):
                # Preserva colunas de metadados como estão
                new_columns.append(col_str)
            else:
                # Sanitiza colunas normais
                sanitized = col_str.replace(" ", "_")
                sanitized = "".join(c for c in sanitized if c.isalnum() or c == "_")
                sanitized = sanitized.lower()
                # Remove underscores múltiplos
                while "__" in sanitized:
                    sanitized = sanitized.replace("__", "_")
                # Remove underscore no início/fim (exceto metadados)
                sanitized = sanitized.strip("_")
                new_columns.append(sanitized or f"col_{len(new_columns)}")

        df_copy.columns = new_columns
        return df_copy

    def _clean_fragmented_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove linhas fragmentadas/órfãs dos dados extraídos.

        Args:
            df: DataFrame com dados potencialmente fragmentados

        Returns:
            DataFrame limpo sem dados órfãos
        """
        if df.empty:
            return df

        df_clean = df.copy()

        # Identificar colunas principais (excluindo metadados)
        main_columns = [col for col in df_clean.columns if not str(col).startswith("_")]

        # Filtros para identificar linhas válidas
        valid_rows = []

        for idx, row in df_clean.iterrows():
            # Converter valores para string e remover espaços
            row_values = {col: str(row[col]).strip() for col in main_columns}

            # Contar campos não vazios
            non_empty_fields = sum(
                1 for val in row_values.values() if val and val != "nan"
            )

            # Definir variáveis para verificar tanto 'team'/'Team' e 'reason'/'Reason'
            team_val = row_values.get("team", "") or row_values.get("Team", "")
            reason_val = row_values.get("reason", "") or row_values.get("Reason", "")

            # Critérios para linha válida:
            is_valid = False

            # 1. Linha tem player_name OU team preenchido (dados primários)
            if row_values.get("player_name", "") or team_val:
                is_valid = True

            # 2. Linha tem pelo menos 3 campos preenchidos (dados consistentes)
            elif non_empty_fields >= 3:
                is_valid = True

            # 3. Linha tem combinação de campos essenciais
            elif (
                row_values.get("game_date", "") and row_values.get("matchup", "")
            ) or (row_values.get("current_status", "") and reason_val):
                is_valid = True

            # 4. NOVO: Preservar times com "NOT YET SUBMITTED" (mesmo se só têm 2 campos)
            elif team_val and reason_val == "NOT YET SUBMITTED":
                is_valid = True

            # 5. Filtrar linhas que são apenas fragmentos de texto órfão
            # (apenas reason preenchido sem outros dados essenciais)
            if (
                non_empty_fields == 1
                and row_values.get("reason", "")
                and not row_values.get("player_name", "")
                and not row_values.get("team", "")
                and not row_values.get("game_date", "")
                and not row_values.get("matchup", "")
            ):
                is_valid = False

            if is_valid:
                valid_rows.append(idx)

        # Manter apenas linhas válidas
        df_filtered = df_clean.loc[valid_rows].copy().reset_index(drop=True)

        return df_filtered

    def _consolidate_related_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Consolida linhas relacionadas que foram fragmentadas na extração.
        Padrão específico para injury reports:
        - Linha órfã com reason (sem player_name) - deve ser anexada ao jogador ANTERIOR
        - Linha com jogador (pode ter reason vazio) - pega linhas órfãs DEPOIS dele

        Args:
            df: DataFrame com dados potencialmente fragmentados

        Returns:
            DataFrame com linhas relacionadas consolidadas
        """
        if df.empty:
            return df

        df_work = df.copy().reset_index(drop=True)
        main_columns = [col for col in df_work.columns if not str(col).startswith("_")]

        # Identificar colunas principais
        player_col = "player_name"
        reason_col = "reason"
        team_col = "team"
        game_date_col = "game_date"
        matchup_col = "matchup"

        consolidated_rows = []
        i = 0

        while i < len(df_work):
            current_row = df_work.iloc[i]
            current_values = {
                col: str(current_row[col]).strip() for col in main_columns
            }

            # Se linha atual tem player_name, é um jogador principal
            if current_values.get(player_col, ""):
                # Iniciar nova linha consolidada
                consolidated_row = current_row.copy()
                consolidated_reasons = []

                # 1. Procurar linhas órfãs ANTES (para jogadores que precisam)
                # Só para casos especiais como LaRavia, Jake
                if i > 0:
                    prev_row = df_work.iloc[i - 1]
                    prev_values = {
                        col: str(prev_row[col]).strip() for col in main_columns
                    }

                    # Se linha anterior é órfã com reason, anexar
                    if (
                        prev_values.get(reason_col, "")
                        and not prev_values.get(player_col, "")
                        and not prev_values.get(team_col, "")
                        and not prev_values.get(game_date_col, "")
                        and not prev_values.get(matchup_col, "")
                    ):
                        consolidated_reasons.append(prev_values[reason_col])

                # 2. Adicionar reason da linha atual se existir
                if current_values.get(reason_col, ""):
                    consolidated_reasons.append(current_values[reason_col])

                # 3. Procurar linhas órfãs DEPOIS (até encontrar próximo jogador)
                j = i + 1
                while j < len(df_work):
                    next_row = df_work.iloc[j]
                    next_values = {
                        col: str(next_row[col]).strip() for col in main_columns
                    }

                    # Se próxima linha tem player_name, parar (encontrou próximo jogador)
                    if next_values.get(player_col, ""):
                        break

                    # Se próxima linha tem team ou game_date, parar (encontrou cabeçalho)
                    if (
                        next_values.get(team_col, "")
                        or next_values.get(game_date_col, "")
                        or next_values.get(matchup_col, "")
                    ):
                        break

                    # Se próxima linha só tem reason (linha órfã), anexar
                    if (
                        next_values.get(reason_col, "")
                        and not next_values.get(player_col, "")
                        and not next_values.get(team_col, "")
                        and not next_values.get(game_date_col, "")
                        and not next_values.get(matchup_col, "")
                    ):
                        consolidated_reasons.append(next_values[reason_col])
                        j += 1
                    else:
                        # Se tem outros dados, parar
                        break

                # Atualizar reason consolidado
                if consolidated_reasons:
                    consolidated_row[reason_col] = "; ".join(consolidated_reasons)

                consolidated_rows.append(consolidated_row)
                i = j  # Pular para a próxima linha não processada

            else:
                # Linha sem player_name - verificar se é linha órfã que deve ser ignorada
                if (
                    current_values.get(reason_col, "")
                    and not current_values.get(player_col, "")
                    and not current_values.get(team_col, "")
                    and not current_values.get(game_date_col, "")
                    and not current_values.get(matchup_col, "")
                ):
                    # Linha órfã - será anexada ao jogador anterior, então ignorar aqui
                    i += 1
                else:
                    # Outro tipo de linha (cabeçalho, etc.) - manter como está
                    consolidated_rows.append(current_row)
                    i += 1

        if consolidated_rows:
            return pd.DataFrame(consolidated_rows)
        else:
            return df

    def _consolidate_related_rows_improved(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Versão melhorada para consolidar linhas relacionadas.
        Foca em preservar todos os jogadores e suas informações.

        Args:
            df: DataFrame com dados potencialmente fragmentados

        Returns:
            DataFrame com linhas relacionadas consolidadas
        """
        if df.empty:
            return df

        df_work = df.copy().reset_index(drop=True)
        main_columns = [col for col in df_work.columns if not str(col).startswith("_")]

        # Identificar colunas principais
        player_col = "Player Name"
        team_col = "Team"
        game_date_col = "Game Date"
        matchup_col = "Matchup"
        game_time_col = "Game Time"
        status_col = "Current Status"

        consolidated_rows = []
        i = 0

        while i < len(df_work):
            current_row = df_work.iloc[i]
            current_values = {
                col: str(current_row[col]).strip() for col in main_columns
            }

            # Se linha atual tem player_name, é um jogador principal
            if current_values.get(player_col, ""):
                # Iniciar nova linha consolidada
                consolidated_row = current_row.copy()

                # Procurar linhas órfãs DEPOIS (até encontrar próximo jogador ou cabeçalho)
                j = i + 1
                while j < len(df_work):
                    next_row = df_work.iloc[j]
                    next_values = {
                        col: str(next_row[col]).strip() for col in main_columns
                    }

                    # Se próxima linha tem player_name, parar (encontrou próximo jogador)
                    if next_values.get(player_col, ""):
                        break

                    # Se próxima linha tem team, game_date, ou matchup, parar (encontrou cabeçalho)
                    if (
                        next_values.get(team_col, "")
                        or next_values.get(game_date_col, "")
                        or next_values.get(matchup_col, "")
                        or next_values.get(game_time_col, "")
                    ):
                        break

                    # Se próxima linha tem status mas não tem player_name, pode ser continuação
                    if (
                        next_values.get(status_col, "")
                        and not next_values.get(player_col, "")
                        and not next_values.get(team_col, "")
                        and not next_values.get(game_date_col, "")
                        and not next_values.get(matchup_col, "")
                        and not next_values.get(game_time_col, "")
                    ):
                        # Pode ser uma linha de status adicional, ignorar
                        j += 1
                    else:
                        # Se tem outros dados, parar
                        break

                consolidated_rows.append(consolidated_row)
                i = j  # Pular para a próxima linha não processada

            else:
                # Linha sem player_name - verificar se é cabeçalho ou linha órfã
                if (
                    current_values.get(team_col, "")
                    or current_values.get(game_date_col, "")
                    or current_values.get(matchup_col, "")
                    or current_values.get(game_time_col, "")
                ):
                    # É um cabeçalho - manter
                    consolidated_rows.append(current_row)
                    i += 1
                else:
                    # Linha órfã - ignorar
                    i += 1

        if consolidated_rows:
            return pd.DataFrame(consolidated_rows)
        else:
            return df

    def _clean_fragmented_data_improved(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Versão melhorada para limpar dados fragmentados.
        Foca em preservar todos os jogadores válidos.

        Args:
            df: DataFrame com dados potencialmente fragmentados

        Returns:
            DataFrame limpo sem dados órfãos
        """
        if df.empty:
            return df

        df_clean = df.copy()

        # Identificar colunas principais (excluindo metadados)
        main_columns = [col for col in df_clean.columns if not str(col).startswith("_")]

        # Filtros para identificar linhas válidas
        valid_rows = []

        for idx, row in df_clean.iterrows():
            # Converter valores para string e remover espaços
            row_values = {col: str(row[col]).strip() for col in main_columns}

            # Contar campos não vazios
            non_empty_fields = sum(
                1 for val in row_values.values() if val and val != "nan"
            )

            # Definir variáveis para verificar colunas principais
            player_val = row_values.get("Player Name", "")
            team_val = row_values.get("Team", "")
            game_date_val = row_values.get("Game Date", "")
            matchup_val = row_values.get("Matchup", "")
            game_time_val = row_values.get("Game Time", "")
            status_val = row_values.get("Current Status", "")

            # Critérios para linha válida:
            is_valid = False

            # 1. Linha tem player_name (dados primários de jogador)
            if player_val:
                is_valid = True

            # 2. Linha tem team (cabeçalho de time)
            elif team_val:
                is_valid = True

            # 3. Linha tem game_date e matchup (cabeçalho de jogo)
            elif game_date_val and matchup_val:
                is_valid = True

            # 4. Linha tem game_time e matchup (cabeçalho de jogo)
            elif game_time_val and matchup_val:
                is_valid = True

            # 5. Linha tem pelo menos 3 campos preenchidos (dados consistentes)
            elif non_empty_fields >= 3:
                is_valid = True

            # 6. Filtrar linhas que são apenas fragmentos de texto órfão
            # (apenas status preenchido sem outros dados essenciais)
            if (
                non_empty_fields == 1
                and status_val
                and not player_val
                and not team_val
                and not game_date_val
                and not matchup_val
                and not game_time_val
            ):
                is_valid = False

            if is_valid:
                valid_rows.append(idx)

        # Manter apenas linhas válidas
        df_filtered = df_clean.loc[valid_rows].copy().reset_index(drop=True)

        return df_filtered

    def get_all_players_from_pdf(self) -> pd.DataFrame:
        """
        Extrai todos os jogadores de todas as páginas do PDF focando apenas em player_name e current_status.

        Returns:
            DataFrame com todos os jogadores encontrados
        """
        all_players = []

        # Processar cada página individualmente
        for page_num in range(1, 10):  # Tentar até 10 páginas
            try:
                # Extrair tabelas da página
                page_tables = camelot.read_pdf(
                    self.file_name,
                    flavor="stream",
                    pages=str(page_num),
                    strip_text="\n",
                )

                if not page_tables:
                    continue

                print(
                    f"Processando página {page_num}: {len(page_tables)} tabelas encontradas"
                )

                # Processar cada tabela da página
                for table_idx, table in enumerate(page_tables):
                    df = table.df.copy()

                    # Procurar por jogadores nesta tabela
                    page_players = self._extract_players_from_table(df, page_num)
                    all_players.extend(page_players)

            except Exception as e:
                print(f"Erro na página {page_num}: {e}")
                continue  # Continuar para a próxima página em vez de parar

        if not all_players:
            return pd.DataFrame(columns=["player_name", "current_status"])

        # Criar DataFrame final
        result_df = pd.DataFrame(all_players)

        # Remover duplicatas por player_name
        result_df = result_df.drop_duplicates(subset=["player_name"], keep="first")

        # Ordenar alfabeticamente por player_name para facilitar
        result_df = result_df.sort_values("player_name").reset_index(drop=True)

        return result_df

    def _extract_players_from_table(self, df: pd.DataFrame, page_num: int) -> list:
        """
        Extrai jogadores de uma tabela específica incluindo player_name e current_status.

        Args:
            df: DataFrame da tabela
            page_num: Número da página

        Returns:
            Lista de dicionários com dados dos jogadores
        """
        players = []

        if df.empty:
            return players

        # Verificar se a primeira linha contém cabeçalhos
        first_row = df.iloc[0]
        has_headers = False

        # Verificar se a primeira linha contém cabeçalhos típicos
        first_row_values = [str(cell).strip().lower() for cell in first_row]
        if any("player name" in val for val in first_row_values) or any(
            "current status" in val for val in first_row_values
        ):
            has_headers = True

        if has_headers:
            # Usar a primeira linha como cabeçalho
            first_row.tolist()
            df_clean = df.iloc[1:].reset_index(drop=True)
        else:
            # Sem cabeçalhos, usar a tabela como está
            df_clean = df.copy()

        # Processar cada linha procurando por jogadores
        for idx, row in df_clean.iterrows():
            # Procurar por jogadores em todas as colunas
            player_found = False
            player_name = ""
            current_status = ""

            # Primeiro, procurar por jogadores
            for col_idx in range(len(row)):
                cell_value = str(row.iloc[col_idx]).strip()

                # Verificar se é um jogador (padrão: "Sobrenome, Nome")
                if "," in cell_value and len(cell_value.split(",")) == 2:
                    parts = cell_value.split(",")
                    if len(parts[0].strip()) > 2 and len(parts[1].strip()) > 1:
                        player_found = True
                        player_name = cell_value
                        break

            if player_found:
                # Procurar por status na mesma linha
                for col_idx in range(len(row)):
                    cell_value = str(row.iloc[col_idx]).strip()
                    if cell_value in [
                        "Out",
                        "Available",
                        "Questionable",
                        "Probable",
                        "Doubtful",
                    ]:
                        current_status = cell_value
                        break

                # Se não encontrou status na mesma linha, procurar em linhas próximas
                if not current_status:
                    for offset in [-1, 1]:
                        check_idx = idx + offset
                        if 0 <= check_idx < len(df_clean):
                            check_row = df_clean.iloc[check_idx]
                            for col_idx in range(len(check_row)):
                                status_value = str(check_row.iloc[col_idx]).strip()
                                if status_value in [
                                    "Out",
                                    "Available",
                                    "Questionable",
                                    "Probable",
                                    "Doubtful",
                                ]:
                                    current_status = status_value
                                    break
                            if current_status:
                                break

                # Criar dados do jogador (player_name e current_status)
                player_data = {
                    "player_name": player_name,
                    "current_status": current_status,
                }

                players.append(player_data)

        return players

    def process_pdf_to_bigquery(
        self, source_file: str, project_id: str, dataset_id: str, table_id: str
    ) -> Optional[int]:
        """
        Processa o PDF atual e carrega diretamente no BigQuery.

        Args:
            source_file: Nome original do arquivo para metadados
            project_id: ID do projeto GCP
            dataset_id: ID do dataset BigQuery
            table_id: ID da tabela BigQuery

        Returns:
            Número de linhas processadas ou None se erro
        """
        try:
            # 1. Extrair dados do PDF
            df = self.get_table_data(fix=True)

            if df.empty:
                print(" (0 linhas extraídas)")
                return 0

            # 2. Sanitizar nomes das colunas
            df = self.sanitize_column_names(df)

            # 3. Carregar no BigQuery usando SmartbettingLib
            from lib_dev.smartbetting import SmartbettingLib

            smartbetting = SmartbettingLib()

            smartbetting.upload_to_bigquery(
                data=df,
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=table_id,
                write_disposition="WRITE_APPEND",
                source_file=source_file,
            )

            return len(df)

        except Exception as e:
            # Log detalhado do erro específico
            error_str = str(e).lower()
            if "camelot" in error_str or "table" in error_str:
                print(f" (Erro extração: {str(e)[:50]}...)")
            elif "bigquery" in error_str or "upload" in error_str:
                print(f" (Erro BigQuery: {str(e)[:50]}...)")
            elif "permission" in error_str or "access" in error_str:
                print(f" (Erro permissão: {str(e)[:50]}...)")
            else:
                print(f" (Erro: {str(e)[:50]}...)")
            return None
