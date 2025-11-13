# ==========================================
# Controle de Estoque — JIOR BLANC
# Streamlit + PostgreSQL/Supabase
# - Movimentação com saldo antes/depois
# - Parser de PDF baseado na função do usuário (processar_pdf_vendas)
# - Autocomplete e ranking por ITEM
# ==========================================
import os
import re
import io
import shutil
import sqlite3
import datetime
from datetime import datetime as dt
from typing import Optional, Tuple, List, Dict
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError

# ==========================================
# Config & Database - POSTGRESQL
# ==========================================
st.set_page_config(page_title="Estoque BOSS BLANC", page_icon="📦", layout="wide")

def get_db_engine():
    """Retorna engine SQLAlchemy para PostgreSQL ou SQLite"""
    try:
        if 'connections' in st.secrets and 'supabase' in st.secrets.connections:
            # PostgreSQL no Supabase
            db_url = st.secrets.connections.supabase.url
            return create_engine(db_url, pool_pre_ping=True)
        else:
            # SQLite local (fallback)
            BASE_DIR = os.path.dirname(os.path.abspath(__file__))
            DATA_DIR = os.path.join(BASE_DIR, "data")
            os.makedirs(DATA_DIR, exist_ok=True)
            DB_PATH = os.path.join(DATA_DIR, "estoque.db")
            return create_engine(f"sqlite:///{DB_PATH}")
    except Exception as e:
        st.error(f"Erro ao conectar ao banco: {e}")
        return None

def get_conn():
    """Retorna conexão para operações raw"""
    engine = get_db_engine()
    if engine:
        return engine.connect()
    return None

def execute_sql(sql, params=None):
    """Executa SQL e retorna DataFrame"""
    try:
        with get_conn() as conn:
            if params:
                result = conn.execute(text(sql), params)
            else:
                result = conn.execute(text(sql))
            
            if result.returns_rows:
                return pd.DataFrame(result.fetchall(), columns=result.keys())
            else:
                conn.commit()
                return None
    except Exception as e:
        st.error(f"Erro executando SQL: {e}")
        return None

def is_postgres():
    """Verifica se está usando PostgreSQL"""
    engine = get_db_engine()
    return engine and 'postgresql' in str(engine.url)

# ==========================================
# Database Initialization
# ==========================================
@st.cache_resource(show_spinner=False)
def init_db() -> None:
    """Inicializa o banco de dados"""
    try:
        engine = get_db_engine()
        if not engine:
            st.error("Não foi possível conectar ao banco de dados")
            return
            
        with engine.connect() as conn:
            is_pg = is_postgres()
            
            if is_pg:
                # PostgreSQL Tables
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS products (
                        id SERIAL PRIMARY KEY,
                        category TEXT NOT NULL,
                        subtype TEXT NOT NULL,
                        sku_base TEXT,
                        custo_unitario REAL DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(category, subtype)
                    );
                """))
                
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS variants (
                        id SERIAL PRIMARY KEY,
                        product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
                        color TEXT NOT NULL,
                        size TEXT NOT NULL,
                        sku TEXT NOT NULL UNIQUE,
                        custo_unitario REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """))
                
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS movements (
                        id SERIAL PRIMARY KEY,
                        variant_id INTEGER NOT NULL REFERENCES variants(id) ON DELETE CASCADE,
                        qty INTEGER NOT NULL,
                        reason TEXT NOT NULL,
                        ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """))
                
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS sku_mapping (
                        id SERIAL PRIMARY KEY,
                        sku_pdf TEXT NOT NULL UNIQUE,
                        sku_estoque TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """))
            else:
                # SQLite Tables
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS products (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        category TEXT NOT NULL,
                        subtype TEXT NOT NULL,
                        sku_base TEXT,
                        custo_unitario REAL DEFAULT 0,
                        UNIQUE(category, subtype)
                    );
                """))
                
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS variants (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
                        color TEXT NOT NULL,
                        size TEXT NOT NULL,
                        sku TEXT NOT NULL UNIQUE,
                        custo_unitario REAL
                    );
                """))
                
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS movements (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        variant_id INTEGER NOT NULL REFERENCES variants(id) ON DELETE CASCADE,
                        qty INTEGER NOT NULL,
                        reason TEXT NOT NULL,
                        ts TEXT NOT NULL
                    );
                """))
                
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS sku_mapping (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        sku_pdf TEXT NOT NULL UNIQUE,
                        sku_estoque TEXT NOT NULL REFERENCES variants(sku) ON DELETE CASCADE
                    );
                """))
            
            conn.commit()
            
        # Create Views
        create_views()
        st.success("Banco de dados inicializado com sucesso!")
        
    except Exception as e:
        st.error(f"Erro ao inicializar banco: {e}")

def create_views():
    """Cria as views do sistema"""
    try:
        with get_conn() as conn:
            is_pg = is_postgres()
            
            # Stock View
            conn.execute(text("DROP VIEW IF EXISTS stock_view"))
            conn.execute(text("""
                CREATE VIEW stock_view AS
                SELECT v.id AS variant_id, v.sku, 
                       COALESCE(SUM(m.qty), 0) AS stock
                FROM variants v
                LEFT JOIN movements m ON m.variant_id = v.id
                GROUP BY v.id, v.sku;
            """))
            
            # Stock Value View
            conn.execute(text("DROP VIEW IF EXISTS stock_value_view"))
            conn.execute(text("""
                CREATE VIEW stock_value_view AS
                SELECT v.sku, p.category, p.subtype, v.color, v.size,
                       COALESCE(SUM(m.qty), 0) AS estoque,
                       COALESCE(v.custo_unitario, p.custo_unitario, 0) AS custo_unitario,
                       (COALESCE(SUM(m.qty), 0) * COALESCE(v.custo_unitario, p.custo_unitario, 0)) AS valor_estoque
                FROM variants v
                JOIN products p ON p.id = v.product_id
                LEFT JOIN movements m ON m.variant_id = v.id
                GROUP BY v.id, v.sku, p.category, p.subtype, v.color, v.size, 
                         p.custo_unitario, v.custo_unitario;
            """))
            
            conn.commit()
    except Exception as e:
        st.error(f"Erro criando views: {e}")

def migrate_db() -> None:
    """Migra o banco de dados para a versão mais recente"""
    try:
        with get_conn() as conn:
            # Verifica e adiciona colunas se necessário
            inspector = inspect(get_db_engine())
            
            # sku_base em products
            if 'products' in inspector.get_table_names():
                columns = [col['name'] for col in inspector.get_columns('products')]
                if 'sku_base' not in columns:
                    conn.execute(text("ALTER TABLE products ADD COLUMN sku_base TEXT"))
                    st.info("✓ Coluna sku_base adicionada à tabela products")
                
                if 'custo_unitario' not in columns:
                    conn.execute(text("ALTER TABLE products ADD COLUMN custo_unitario REAL DEFAULT 0"))
                    st.info("✓ Coluna custo_unitario adicionada à tabela products")
            
            # custo_unitario em variants
            if 'variants' in inspector.get_table_names():
                columns = [col['name'] for col in inspector.get_columns('variants')]
                if 'custo_unitario' not in columns:
                    conn.execute(text("ALTER TABLE variants ADD COLUMN custo_unitario REAL"))
                    st.info("✓ Coluna custo_unitario adicionada à tabela variants")
            
            conn.commit()
            create_views()
            
    except Exception as e:
        st.error(f"Erro na migração: {e}")

# ==========================================
# Helpers: SKU
# ==========================================
def generate_sku(sku_base: str, color: str, size: str) -> str:
    """Gera SKU no formato: SKUBASE-Cor-Tamanho (mantendo hífens do sku_base)"""
    cor_limpa = re.sub(r'[^a-zA-Z0-9ÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇáàâãéèêíìîóòôõúùûç ]', '', color.strip()).strip().title().replace(" ", "")
    tamanho_limpo = re.sub(r'[^A-Za-z0-9]', '', size.strip().upper())
    sku_base_limpo = sku_base.strip().upper().replace(" ", "")
    return f"{sku_base_limpo}-{cor_limpa}-{tamanho_limpo}"

def sanitize_sku(s: str) -> str:
    s = (s or "").strip().upper().replace(" ", "")
    return re.sub(r"[^A-Z0-9\-_ÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇ]", "", s)

def normalize_key(s: str) -> str:
    """Normaliza para comparar: remove tudo exceto A-Z0-9"""
    return re.sub(r'[^A-Z0-9]', '', sanitize_sku(s))

def sanitized_to_original_sku_map() -> Dict[str, str]:
    """Retorna {SKU_sanitizado: SKU_original_no_banco} para usar o case exato ao gravar."""
    vdf = list_variants_df()
    orig_list = vdf["sku"].astype(str).tolist()
    return {sanitize_sku(s): s for s in orig_list}

# ==========================================
# CRUD Operations
# ==========================================
def get_or_create_product(category: str, subtype: str, sku_base: Optional[str] = None, custo_unitario: float = 0) -> int:
    """Obtém ou cria um produto"""
    try:
        with get_conn() as conn:
            # Tenta encontrar produto existente
            result = conn.execute(
                text("SELECT id FROM products WHERE category = :category AND subtype = :subtype"),
                {"category": category.strip(), "subtype": subtype.strip()}
            )
            row = result.fetchone()
            
            if row:
                product_id = row[0]
                # Atualiza se necessário
                if sku_base is not None or custo_unitario is not None:
                    conn.execute(
                        text("UPDATE products SET sku_base = :sku_base, custo_unitario = :custo WHERE id = :id"),
                        {"sku_base": sku_base.strip() if sku_base else None, 
                         "custo": custo_unitario if custo_unitario is not None else 0,
                         "id": product_id}
                    )
                return product_id
            else:
                # Cria novo produto
                is_pg = is_postgres()
                if is_pg:
                    result = conn.execute(
                        text("""
                            INSERT INTO products (category, subtype, sku_base, custo_unitario) 
                            VALUES (:category, :subtype, :sku_base, :custo) 
                            RETURNING id
                        """),
                        {"category": category.strip(), "subtype": subtype.strip(), 
                         "sku_base": sku_base.strip() if sku_base else None, 
                         "custo": custo_unitario or 0}
                    )
                    product_id = result.scalar()
                else:
                    result = conn.execute(
                        text("""
                            INSERT INTO products (category, subtype, sku_base, custo_unitario) 
                            VALUES (:category, :subtype, :sku_base, :custo)
                        """),
                        {"category": category.strip(), "subtype": subtype.strip(), 
                         "sku_base": sku_base.strip() if sku_base else None, 
                         "custo": custo_unitario or 0}
                    )
                    product_id = result.lastrowid
                
                conn.commit()
                return product_id
    except Exception as e:
        st.error(f"Erro em get_or_create_product: {e}")
        return None

def create_variant(category: str, subtype: str, color: str, size: str, sku_base: Optional[str] = None, sku_override: Optional[str] = None, custo_unitario_produto: float = 0, custo_unitario_variante: Optional[float] = None) -> Tuple[bool, str]:
    """Cria variante e permite custo da VARIANTE (opcional)."""
    try:
        with get_conn() as conn:
            product_id = get_or_create_product(category, subtype, sku_base, custo_unitario_produto or 0)
            
            if not sku_base:
                try:
                    result = conn.execute(
                        text("SELECT sku_base FROM products WHERE id = :product_id"),
                        {"product_id": product_id}
                    )
                    sku_base_row = result.fetchone()
                    sku_base = sku_base_row[0] if sku_base_row and sku_base_row[0] else None
                except Exception:
                    sku_base = None
            
            if sku_base:
                sku_auto = generate_sku(sku_base, color, size)
            else:
                def part(x: str, n: int) -> str:
                    return x.strip()[:n].upper() if x else "X"
                sku_auto = f"{part(category,4)}-{part(subtype,4)}-{part(color,3)}-{part(size,4)}"
            
            sku = sanitize_sku(sku_override or sku_auto)
            
            is_pg = is_postgres()
            if is_pg:
                conn.execute(
                    text("""
                        INSERT INTO variants (product_id, color, size, sku, custo_unitario) 
                        VALUES (:product_id, :color, :size, :sku, :custo)
                    """),
                    {
                        "product_id": product_id,
                        "color": color.strip(),
                        "size": size.strip(),
                        "sku": sku,
                        "custo": float(custo_unitario_variante) if custo_unitario_variante else None
                    }
                )
            else:
                conn.execute(
                    text("""
                        INSERT INTO variants (product_id, color, size, sku, custo_unitario) 
                        VALUES (:product_id, :color, :size, :sku, :custo)
                    """),
                    {
                        "product_id": product_id,
                        "color": color.strip(),
                        "size": size.strip(),
                        "sku": sku,
                        "custo": float(custo_unitario_variante) if custo_unitario_variante else None
                    }
                )
            
            conn.commit()
            return True, sku
            
    except Exception as e:
        return False, f"Erro ao criar variante: {e}"

def record_movement(sku: str, qty: int, reason: str) -> None:
    """Registra movimentação no estoque"""
    try:
        with get_conn() as conn:
            result = conn.execute(
                text("SELECT id FROM variants WHERE sku = :sku"),
                {"sku": sku}
            )
            row = result.fetchone()
            
            if not row:
                raise ValueError("SKU não encontrado.")
            
            variant_id = row[0]
            
            is_pg = is_postgres()
            if is_pg:
                conn.execute(
                    text("""
                        INSERT INTO movements (variant_id, qty, reason, ts) 
                        VALUES (:variant_id, :qty, :reason, NOW())
                    """),
                    {
                        "variant_id": variant_id,
                        "qty": qty,
                        "reason": reason
                    }
                )
            else:
                conn.execute(
                    text("""
                        INSERT INTO movements (variant_id, qty, reason, ts) 
                        VALUES (:variant_id, :qty, :reason, :ts)
                    """),
                    {
                        "variant_id": variant_id,
                        "qty": qty,
                        "reason": reason,
                        "ts": datetime.datetime.now().isoformat(timespec="seconds")
                    }
                )
            
            conn.commit()
            
    except Exception as e:
        raise ValueError(f"Erro ao registrar movimentação: {e}")

def update_variant(old_sku: str, new_sku: str, category: str, subtype: str, color: str, size: str, sku_base: Optional[str] = None, custo_unitario_produto: Optional[float] = None, custo_unitario_variante: Optional[float] = None) -> Tuple[bool, str]:
    """Atualiza dados da variante"""
    try:
        with get_conn() as conn:
            result = conn.execute(
                text("SELECT id, product_id FROM variants WHERE sku = :sku"),
                {"sku": old_sku}
            )
            variant = result.fetchone()
            
            if not variant:
                return False, "SKU não encontrado."
            
            variant_id, old_product_id = variant
            
            if old_sku != new_sku:
                result = conn.execute(
                    text("SELECT id FROM variants WHERE sku = :sku"),
                    {"sku": new_sku}
                )
                if result.fetchone():
                    return False, "Novo SKU já existe no sistema."
            
            new_product_id = get_or_create_product(category, subtype, sku_base, custo_unitario_produto if custo_unitario_produto is not None else 0)
            
            conn.execute(
                text("""
                    UPDATE variants 
                    SET sku = :new_sku, color = :color, size = :size, 
                        product_id = :product_id, custo_unitario = :custo 
                    WHERE id = :variant_id
                """),
                {
                    "new_sku": new_sku,
                    "color": color.strip(),
                    "size": size.strip(),
                    "product_id": new_product_id,
                    "custo": float(custo_unitario_variante) if custo_unitario_variante is not None else None,
                    "variant_id": variant_id
                }
            )
            
            # Remove produto antigo se não há mais variantes
            result = conn.execute(
                text("SELECT COUNT(*) FROM variants WHERE product_id = :product_id"),
                {"product_id": old_product_id}
            )
            if result.fetchone()[0] == 0:
                conn.execute(
                    text("DELETE FROM products WHERE id = :product_id"),
                    {"product_id": old_product_id}
                )
            
            conn.commit()
            return True, "Variante atualizada com sucesso!"
            
    except Exception as e:
        return False, f"Erro ao atualizar variante: {e}"

def update_sku_base_bulk(category: str, subtype: str, new_sku_base: str) -> Tuple[bool, str]:
    """Atualiza o SKU base de um produto e regenera todos os SKUs das variantes"""
    try:
        with get_conn() as conn:
            result = conn.execute(
                text("SELECT id FROM products WHERE category = :category AND subtype = :subtype"),
                {"category": category, "subtype": subtype}
            )
            product = result.fetchone()
            
            if not product:
                return False, "Produto não encontrado."
            
            product_id = product[0]
            
            result = conn.execute(
                text("SELECT id, color, size FROM variants WHERE product_id = :product_id"),
                {"product_id": product_id}
            )
            variants = result.fetchall()
            
            for variant_id, color, size in variants:
                new_sku = generate_sku(new_sku_base, color, size)
                conn.execute(
                    text("UPDATE variants SET sku = :new_sku WHERE id = :variant_id"),
                    {"new_sku": new_sku, "variant_id": variant_id}
                )
            
            conn.execute(
                text("UPDATE products SET sku_base = :sku_base WHERE id = :product_id"),
                {"sku_base": new_sku_base, "product_id": product_id}
            )
            
            conn.commit()
            return True, f"SKU base atualizado e {len(variants)} variantes regeneradas com sucesso!"
            
    except Exception as e:
        return False, f"Erro ao atualizar SKU base: {e}"

def update_custo_unitario(category: str, subtype: str, novo_custo: float) -> Tuple[bool, str]:
    """Atualiza custo no PRODUTO"""
    try:
        with get_conn() as conn:
            result = conn.execute(
                text("SELECT id FROM products WHERE category = :category AND subtype = :subtype"),
                {"category": category, "subtype": subtype}
            )
            product = result.fetchone()
            
            if not product:
                return False, "Produto não encontrado."
            
            product_id = product[0]
            
            conn.execute(
                text("UPDATE products SET custo_unitario = :custo WHERE id = :product_id"),
                {"custo": novo_custo, "product_id": product_id}
            )
            
            conn.commit()
            return True, f"Custo unitário (PRODUTO) atualizado para R$ {novo_custo:.2f}"
            
    except Exception as e:
        return False, f"Erro ao atualizar custo unitário: {e}"

def delete_variant(sku: str) -> Tuple[bool, str]:
    """Remove uma variante"""
    try:
        with get_conn() as conn:
            result = conn.execute(
                text("SELECT id, product_id FROM variants WHERE sku = :sku"),
                {"sku": sku}
            )
            variant = result.fetchone()
            
            if not variant:
                return False, "SKU não encontrado."
            
            variant_id, product_id = variant
            
            # Remove mapeamentos
            try:
                conn.execute(
                    text("DELETE FROM sku_mapping WHERE sku_estoque = :sku"),
                    {"sku": sku}
                )
            except:
                pass
            
            # Remove variante
            conn.execute(
                text("DELETE FROM variants WHERE id = :variant_id"),
                {"variant_id": variant_id}
            )
            
            # Remove produto se não há mais variantes
            result = conn.execute(
                text("SELECT COUNT(*) FROM variants WHERE product_id = :product_id"),
                {"product_id": product_id}
            )
            if result.fetchone()[0] == 0:
                conn.execute(
                    text("DELETE FROM products WHERE id = :product_id"),
                    {"product_id": product_id}
                )
            
            conn.commit()
            return True, "Variante removida com sucesso!"
            
    except Exception as e:
        return False, f"Erro ao remover variante: {e}"

def get_variant_details(sku: str) -> Optional[dict]:
    """Obtém detalhes de uma variante"""
    try:
        with get_conn() as conn:
            result = conn.execute(
                text("""
                    SELECT v.sku, p.category, p.subtype, v.color, v.size, 
                           v.id, p.id, p.sku_base, p.custo_unitario, v.custo_unitario
                    FROM variants v 
                    JOIN products p ON p.id = v.product_id 
                    WHERE v.sku = :sku
                """),
                {"sku": sku}
            )
            row = result.fetchone()
            
            if row:
                return {
                    'sku': row[0],
                    'category': row[1],
                    'subtype': row[2],
                    'color': row[3],
                    'size': row[4],
                    'variant_id': row[5],
                    'product_id': row[6],
                    'sku_base': row[7],
                    'custo_unitario_produto': row[8] if row[8] is not None else 0,
                    'custo_unitario_variante': row[9]
                }
            return None
    except Exception as e:
        st.error(f"Erro ao obter detalhes da variante: {e}")
        return None

# ==========================================
# Query Functions
# ==========================================
def list_products_df() -> pd.DataFrame:
    """Lista todos os produtos"""
    try:
        return execute_sql("""
            SELECT id, category, subtype, sku_base, custo_unitario 
            FROM products 
            ORDER BY category, subtype
        """)
    except:
        return pd.DataFrame()

def list_variants_df() -> pd.DataFrame:
    """Lista todas as variantes"""
    try:
        return execute_sql("""
            SELECT v.id, v.sku, p.category, p.subtype, v.color, v.size, 
                   p.sku_base, p.custo_unitario AS custo_unitario_produto, 
                   v.custo_unitario AS custo_unitario_variante, p.id as product_id
            FROM variants v 
            JOIN products p ON p.id = v.product_id 
            ORDER BY p.category, p.subtype, v.color, v.size
        """)
    except:
        return pd.DataFrame()

def stock_df(filter_text: Optional[str] = None, critical_only: bool = False, critical_value: int = 0) -> pd.DataFrame:
    """Consulta estoque com filtros"""
    try:
        base_sql = """
            SELECT v.sku, p.category AS categoria, p.subtype AS subtipo, 
                   v.color AS cor, v.size AS tamanho, COALESCE(s.stock,0) AS estoque,
                   COALESCE(v.custo_unitario, p.custo_unitario, 0) AS custo_unitario,
                   (COALESCE(s.stock,0) * COALESCE(v.custo_unitario, p.custo_unitario, 0)) AS valor_estoque
            FROM variants v 
            JOIN products p ON p.id = v.product_id 
            LEFT JOIN stock_view s ON s.variant_id = v.id
        """
        
        conditions = []
        params = {}
        
        if filter_text:
            conditions.append("(v.sku ILIKE :filter OR p.category ILIKE :filter OR p.subtype ILIKE :filter OR v.color ILIKE :filter OR v.size ILIKE :filter)")
            params["filter"] = f"%{filter_text}%"
        
        if critical_only and critical_value > 0:
            conditions.append("COALESCE(s.stock,0) <= :critical")
            params["critical"] = critical_value
        
        if conditions:
            base_sql += " WHERE " + " AND ".join(conditions)
        
        base_sql += " ORDER BY p.category, p.subtype, v.color, v.size"
        
        return execute_sql(base_sql, params)
    except:
        return pd.DataFrame()

def stock_value_df(filter_text: Optional[str] = None) -> pd.DataFrame:
    """Consulta valor do estoque"""
    try:
        base_sql = """
            SELECT sku, category, subtype, color, size, estoque, 
                   custo_unitario, valor_estoque 
            FROM stock_value_view
        """
        
        params = {}
        if filter_text:
            base_sql += " WHERE category ILIKE :filter OR subtype ILIKE :filter OR color ILIKE :filter OR size ILIKE :filter"
            params["filter"] = f"%{filter_text}%"
        
        base_sql += " ORDER BY category, subtype, color, size"
        
        return execute_sql(base_sql, params)
    except:
        return pd.DataFrame()

def movements_df(sku_filter: Optional[str] = None, reason: Optional[str] = None, days: Optional[int] = None) -> pd.DataFrame:
    """Consulta movimentações"""
    try:
        sql = """
            SELECT m.id, v.sku, p.category AS categoria, p.subtype AS subtipo, 
                   v.color AS cor, v.size AS tamanho, m.qty AS quantidade, 
                   m.reason AS motivo, m.ts AS quando
            FROM movements m 
            JOIN variants v ON v.id = m.variant_id 
            JOIN products p ON p.id = v.product_id
        """
        
        conditions = []
        params = {}
        
        if sku_filter:
            conditions.append("v.sku = :sku")
            params["sku"] = sku_filter
        
        if reason and reason != "Todos":
            conditions.append("m.reason = :reason")
            params["reason"] = reason
        
        if days:
            if is_postgres():
                conditions.append("m.ts >= NOW() - INTERVAL ':days days'")
            else:
                ts_min = (datetime.datetime.now() - datetime.timedelta(days=days)).isoformat(timespec="seconds")
                conditions.append("m.ts >= :ts_min")
                params["ts_min"] = ts_min
            params["days"] = days
        
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        
        sql += " ORDER BY m.ts DESC"
        
        return execute_sql(sql, params)
    except:
        return pd.DataFrame()

def get_sales_data(days: Optional[int] = None) -> pd.DataFrame:
    """Obtém dados de vendas"""
    try:
        sql = """
            SELECT p.category, p.subtype, v.color, v.size, 
                   ABS(SUM(m.qty)) as quantidade_vendida,
                   COUNT(*) as numero_vendas,
                   COALESCE(v.custo_unitario, p.custo_unitario, 0) as custo_unitario,
                   (ABS(SUM(m.qty)) * COALESCE(v.custo_unitario, p.custo_unitario, 0)) as valor_total_vendido
            FROM movements m 
            JOIN variants v ON v.id = m.variant_id 
            JOIN products p ON p.id = v.product_id
            WHERE m.reason IN ('venda', 'venda_pdf')
        """
        
        params = {}
        if days:
            if is_postgres():
                sql += " AND m.ts >= NOW() - INTERVAL ':days days'"
            else:
                ts_min = (datetime.datetime.now() - datetime.timedelta(days=days)).isoformat(timespec="seconds")
                sql += " AND m.ts >= :ts_min"
                params["ts_min"] = ts_min
            params["days"] = days
        
        sql += " GROUP BY p.category, p.subtype, v.color, v.size, p.custo_unitario, v.custo_unitario"
        sql += " ORDER BY quantidade_vendida DESC"
        
        return execute_sql(sql, params)
    except:
        return pd.DataFrame()

# ==========================================
# SKU Mapping helpers
# ==========================================
def get_sku_mapping(sku_pdf_norm: str) -> Optional[str]:
    """Obtém mapeamento de SKU"""
    try:
        key_pdf = sanitize_sku(sku_pdf_norm)
        
        # 1) Tabela de mapeamento
        result = execute_sql("SELECT sku_pdf, sku_estoque FROM sku_mapping")
        if result is not None and not result.empty:
            map1 = {sanitize_sku(k): v for (k, v) in result.values}
            if key_pdf in map1:
                return map1[key_pdf]
        
        # 2) Casamento direto por normalização
        vdf = list_variants_df()
        if not vdf.empty:
            norm_index = {normalize_key(sku): sku for sku in vdf["sku"].tolist()}
            cand = norm_index.get(normalize_key(key_pdf))
            return cand
        
        return None
    except:
        return None

# ==========================================
# PDF Parser
# ==========================================
def processar_pdf_vendas(pdf_file) -> Tuple[bool, List[dict], str]:
    """Parser robusto p/ PDF (UpSeller)"""
    try:
        # Try different PDF libraries
        try:
            import pypdf
            reader = pypdf.PdfReader(pdf_file)
        except ImportError:
            try:
                import PyPDF2
                reader = PyPDF2.PdfReader(pdf_file)
            except ImportError:
                st.error("Instale pypdf: pip install pypdf")
                return False, [], "Biblioteca PDF não encontrada"
        
        raw = ""
        for p in reader.pages:
            raw += (p.extract_text() or "") + "\n"
        
        # Rest of your PDF processing code remains the same
        lines = [ln.strip() for ln in raw.replace("\r", "\n").split("\n")]
        lines = [ln for ln in lines if ln]
        lines = [re.sub(r"\s+\d+/\d+\s*$", "", ln) for ln in lines]
        
        skip_re = re.compile("|".join([
            r"^LISTA DE RESUMO",
            r"^\(PRODUTOS DO ARMAZ[EÉ]M\)",
            r"^PRODUTOS DO ARMAZ[EÉ]M",
            r"^VARIA[CÇ][AÃ]O$",
            r"^SKU DE PRODUTO$",
            r"^QTD\.?$",
            r"^IMPRIMIR.*UPSELLER",
            r"^HTTPS?://",
            r"^\d+/\d+$",
            r"^\d{1,2}/\d{1,2}/\d{4}",
            r"^QTD\. DE PEDIDOS",
            r"^N[ÚU]MERO DE SKUS DE PRODUTOS",
            r"^TOTAL DE PRODUTOS",
        ]), re.IGNORECASE)
        
        kept = [ln for ln in lines if not skip_re.search(ln)]
        merged = []
        i = 0
        while i < len(kept):
            cur = kept[i]
            if cur.endswith("-") and i + 1 < len(kept):
                cur = cur + kept[i + 1]
                i += 2
                while cur.endswith("-") and i < len(kept):
                    cur = cur + kept[i]
                    i += 1
                merged.append(cur)
            else:
                merged.append(cur)
                i += 1
        
        SIZE = r"(?:XGG|GG|XG|PP|G|M|P|\d{1,3})"
        TOKEN = (
            r"(?:[A-Z]{2,}(?:-[A-Z]{2,}){0,2})"
            r"-(?:[A-Z0-9ÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇ]+)"
            r"(?:-[A-Z0-9ÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇ]+)?"
            r"-" + SIZE
        )
        sku_pattern = re.compile(rf"({TOKEN})(\d{{1,3}})?", re.UNICODE)
        preface_size_start = re.compile(rf"^(?:{SIZE})(?=(?:[A-Z]{{2,}}(?:-[A-Z]{{2,}}){{0,2}})-)")
        preface_size_after_comma = re.compile(rf",(?:{SIZE})(?=(?:[A-Z]{{2,}}(?:-[A-Z]{{2,}}){{0,2}})-)")
        size_suffix_re = re.compile(rf"^(.*-)(XGG|GG|XG|PP|G|M|P|\d{{1,3}})$")
        recognized_sizes = {'4','6','8','10','12','14','16','P','M','G','GG','PP','XG','XGG'}
        
        def norm(s: str) -> str:
            s = s.upper()
            s = re.sub(r"\s+", "", s)
            s = re.sub(r"-{2,}", "-", s)
            return s
        
        def maybe_int(txt: str, next_char: Optional[str] = None) -> Optional[int]:
            if txt is None:
                return None
            if not re.fullmatch(r"\d{1,3}", txt):
                return None
            if next_char == "/":  # evita pegar parte de data
                return int(txt[0])
            return int(txt)
        
        movimentos: List[dict] = []
        vistos: set = set()
        pending_sku: Optional[str] = None
        
        for ln in merged:
            compact = norm(ln)
            compact = preface_size_start.sub("", compact)
            compact = preface_size_after_comma.sub(",", compact)
            last_end = 0
            
            for m in sku_pattern.finditer(compact):
                token = m.group(1)
                qty_str = m.group(2)
                token_out = token
                ms = size_suffix_re.match(token)
                
                if ms:
                    size_part = ms.group(2)
                    if qty_str is None and re.fullmatch(r"\d{2,3}", size_part) and size_part not in recognized_sizes:
                        take = ""
                        s = size_part
                        while len(s) > 1 and s not in recognized_sizes:
                            take = s[-1] + take
                            s = s[:-1]
                        if take:
                            qty_str = take
                            token_out = ms.group(1) + s
                    if qty_str is None and re.fullmatch(r"\d{3}", size_part):
                        token_out = ms.group(1) + size_part[:2]
                        qty_str = size_part[2:]
                
                next_char = compact[m.end(2)] if (m.end(2) < len(compact) if qty_str else False) else (compact[m.end(1)] if (m.end(1) < len(compact)) else None)
                qty_val = maybe_int(qty_str, next_char) if qty_str else None
                
                if qty_val is not None:
                    sku_n = norm(token_out)
                    key = (sku_n, qty_val)
                    if key not in vistos:
                        vistos.add(key)
                        mapped = get_sku_mapping(sku_n)
                        movimentos.append({
                            "sku_pdf": sku_n,
                            "sku": mapped or sku_n,
                            "quantidade": int(qty_val),
                            "produto": "Extraído do PDF",
                            "variacao": "Extraído do PDF",
                            "mapeado": bool(mapped),
                        })
                    pending_sku = None
                else:
                    pending_sku = token_out
                last_end = m.end()
            
            if pending_sku:
                tail = compact[last_end:]
                if re.fullmatch(r"\d{1,3}", tail or ""):
                    q = maybe_int(tail, None)
                    if q is not None:
                        sku_n = norm(pending_sku)
                        key = (sku_n, q)
                        if key not in vistos:
                            vistos.add(key)
                            mapped = get_sku_mapping(sku_n)
                            movimentos.append({
                                "sku_pdf": sku_n,
                                "sku": mapped or sku_n,
                                "quantidade": int(q),
                                "produto": "Extraído do PDF",
                                "variacao": "Extraído do PDF",
                                "mapeado": bool(mapped),
                            })
                        pending_sku = None
                else:
                    m2 = re.fullmatch(rf"(?:{SIZE})?(\d{{1,3}})", tail or "")
                    if m2:
                        q = int(m2.group(1))
                        sku_n = norm(pending_sku)
                        key = (sku_n, q)
                        if key not in vistos:
                            vistos.add(key)
                            mapped = get_sku_mapping(sku_n)
                            movimentos.append({
                                "sku_pdf": sku_n,
                                "sku": mapped or sku_n,
                                "quantidade": int(q),
                                "produto": "Extraído do PDF",
                                "variacao": "Extraído do PDF",
                                "mapeado": bool(mapped),
                            })
                        pending_sku = None
        
        if not movimentos:
            return False, [], "Nenhum item encontrado no PDF."
        
        return True, movimentos, f"Encontrados {len(movimentos)} itens no PDF"
    
    except Exception as e:
        import traceback
        st.error(f"Erro detalhado: {traceback.format_exc()}")
        return False, [], f"Erro ao processar PDF: {str(e)}"

# ==========================================
# Backup and Migration
# ==========================================
def backup_database():
    """Faz backup do banco de dados"""
    try:
        if is_postgres():
            st.info("Backup automático no PostgreSQL - Supabase cuida dos backups")
            return "backup_automatico_postgres"
        else:
            # Backup SQLite local
            BASE_DIR = os.path.dirname(os.path.abspath(__file__))
            DATA_DIR = os.path.join(BASE_DIR, "data")
            BACKUP_DIR = os.path.join(DATA_DIR, "backups")
            os.makedirs(BACKUP_DIR, exist_ok=True)
            
            timestamp = dt.now().strftime("%Y%m%d_%H%M%S")
            backup_path = os.path.join(BACKUP_DIR, f"estoque_backup_{timestamp}.db")
            
            if os.path.exists(os.path.join(DATA_DIR, "estoque.db")):
                shutil.copy2(os.path.join(DATA_DIR, "estoque.db"), backup_path)
            else:
                open(backup_path, "wb").close()
            
            return backup_path
    except Exception as e:
        st.error(f"Erro no backup: {e}")
        return None

def migrate_from_sqlite_to_postgres():
    """Migra dados do SQLite local para PostgreSQL"""
    try:
        if not is_postgres():
            st.warning("Só é possível migrar quando conectado ao PostgreSQL")
            return
        
        # Conecta ao SQLite local
        local_engine = create_engine("sqlite:///data/estoque.db")
        
        # Conecta ao PostgreSQL
        pg_engine = get_db_engine()
        
        tables = ['products', 'variants', 'movements', 'sku_mapping']
        total_migrated = 0
        
        for table in tables:
            try:
                # Lê dados do SQLite
                with local_engine.connect() as local_conn:
                    df = pd.read_sql_table(table, local_conn)
                
                if not df.empty:
                    # Escreve no PostgreSQL
                    with pg_engine.connect() as pg_conn:
                        df.to_sql(table, pg_conn, if_exists='append', index=False)
                    
                    st.success(f"✅ {table}: {len(df)} registros")
                    total_migrated += len(df)
            except Exception as e:
                st.error(f"❌ {table}: {e}")
        
        if total_migrated > 0:
            st.success(f"🎉 Migração concluída! {total_migrated} registros migrados")
            create_views()
        else:
            st.info("Nenhum dado para migrar")
            
    except Exception as e:
        st.error(f"Erro na migração: {e}")

# ==========================================
# UI START
# ==========================================
def main():
    st.title("📦 Controle de Estoque — BOSS BLANC")
    st.caption("Cadastre produtos, variantes e registre entradas/saídas com histórico e exportação de CSV.")
    
    # Initialize database
    init_db()
    migrate_db()
    
    # Database status
    db_status = "PostgreSQL" if is_postgres() else "SQLite"
    st.sidebar.info(f"📊 Banco: {db_status}")
    
    # ------------- Sidebar -------------
    with st.sidebar:
        st.header("Navegação")
        page = st.radio(
            "Ir para:",
            [
                "Cadastrar Tipo/Subtipo",
                "Cadastrar Variante",
                "Movimentar Estoque",
                "Baixa por PDF",
                "Estoque Atual",
                "Histórico",
                "Exportar CSV",
                "Editar Variante",
                "Remover Variante",
                "Mapeamento de SKUs",
                "Gerenciar SKU Base",
                "Custo por Categoria/Subtipo (em massa)",
                "Contagem de Estoque",
                "Valor do Estoque",
                "Gráfico de Vendas",
                "Migrar Dados"
            ],
            index=3,
        )
        
        st.divider()
        st.markdown("**Dica:** nos selects, digite para filtrar o SKU (autocomplete).")
        
        if st.button("🔄 Forçar Migração do Banco"):
            migrate_db()
            st.success("Migração executada com sucesso!")
            st.rerun()
        
        if st.button("💾 Criar Backup Agora"):
            backup_path = backup_database()
            st.success(f"Backup criado: {backup_path}")
        
        # Migração SQLite → PostgreSQL
        if page == "Migrar Dados":
            if st.button("🚀 Migrar Dados SQLite → PostgreSQL", type="primary"):
                migrate_from_sqlite_to_postgres()
    
    # ==========================================
    # PÁGINAS
    # ==========================================
    
    # -------- Cadastrar Tipo/Subtipo --------
    if page == "Cadastrar Tipo/Subtipo":
        st.subheader("Cadastrar novo tipo de produto")
        col1, col2, col3, col4 = st.columns([2, 2, 2, 2])
        with col1:
            category = st.text_input("Categoria (ex.: short, camiseta, moletom)")
        with col2:
            subtype = st.text_input("Subtipo (ex.: tactel, dryfit, algodão, canguru, careca)")
        with col3:
            sku_base = st.text_input("SKU Base (ex.: MOL-CARECA)", help="Usado para gerar SKUs automaticamente: SKUBASE-Cor-Tamanho")
        with col4:
            custo_unitario = st.number_input("Custo Unitário (PRODUTO) R$", min_value=0.0, value=0.0, step=0.01, help="Custo padrão para este tipo/subtipo")
        
        if st.button("Salvar tipo/subtipo", type="primary"):
            if not category or not subtype:
                st.error("Preencha categoria e subtipo.")
            else:
                _ = get_or_create_product(category, subtype, sku_base, custo_unitario)
                if sku_base:
                    st.success(f"Tipo/Subtipo salvo: {category} / {subtype} com SKU Base: {sku_base} e custo padrão: R$ {custo_unitario:.2f}")
                else:
                    st.success(f"Tipo/Subtipo salvo: {category} / {subtype} com custo padrão: R$ {custo_unitario:.2f}")
        
        st.divider()
        st.subheader("Produtos cadastrados")
        st.dataframe(list_products_df(), use_container_width=True)
    
    # -------- Cadastrar Variante --------
    elif page == "Cadastrar Variante":
        st.subheader("Cadastrar nova variante")
        col1, col2, col3, col4, col5 = st.columns([2,2,2,2,2])
        with col1:
            category = st.text_input("Categoria")
        with col2:
            subtype = st.text_input("Subtipo")
        with col3:
            color = st.text_input("Cor")
        with col4:
            size = st.text_input("Tamanho")
        with col5:
            sku_base = st.text_input("SKU Base (opcional — se vazio, usa SKU Base do produto)")
        
        custo_unitario_produto = st.number_input("Custo Unitário (PRODUTO) R$", min_value=0.0, value=0.0, step=0.01, help="Define/atualiza o custo padrão do produto (categoria/subtipo)")
        custo_unitario_variante = st.number_input("Custo Unitário (VARIANTE) R$ (opcional)", min_value=0.0, value=0.0, step=0.01, help="Se informado > 0, esta variante usará este custo (não afeta as outras)")
        sku_override = st.text_input("SKU (opcional — para sobrepor)")
        
        if st.button("Criar variante", type="primary"):
            cvar = custo_unitario_variante if custo_unitario_variante > 0 else None
            ok, msg = create_variant(category, subtype, color, size, sku_base, sku_override, custo_unitario_produto, cvar)
            if ok:
                st.success(f"Variante criada! SKU: {msg}")
            else:
                st.error(msg)
    
    # -------- Movimentar Estoque (saldo antes/depois) --------
    elif page == "Movimentar Estoque":
        st.subheader("Movimentar Estoque")
        vdf = list_variants_df()
        sku_options = vdf["sku"].tolist() if not vdf.empty else []
        sku = st.selectbox("SKU (digite para filtrar)", sku_options, index=None, placeholder="Digite parte do SKU…")
        
        estoque_atual = None
        if sku:
            df_sku = stock_df(filter_text=sku)
            if not df_sku.empty:
                try:
                    estoque_atual = int(df_sku.loc[df_sku["sku"] == sku, "estoque"].values[0])
                except Exception:
                    estoque_atual = 0
            st.metric("Estoque atual", estoque_atual)
        
        qtd_input = st.number_input("Quantidade", value=1, step=1, min_value=1)
        reason = st.selectbox(
            "Motivo",
            ["entrada", "venda", "venda_pdf", "ajuste"],
            index=0,
            help="Entrada = positivo; Vendas = negativo; Ajuste = você escolhe o sinal."
        )
        
        if reason == "ajuste":
            sinal = st.radio("Sinal do ajuste", ["positivo (+)", "negativo (-)"], horizontal=True, index=0)
            qty_final = qtd_input if sinal == "positivo (+)" else -qtd_input
        else:
            qty_final = qtd_input if reason == "entrada" else -qtd_input
        
        st.caption(f"Quantidade aplicada: **{qty_final}** (motivo: **{reason}**)")
        
        if st.button("Registrar movimentação", type="primary"):
            if not sku:
                st.error("Escolha um SKU.")
            elif qty_final == 0:
                st.error("Quantidade não pode ser zero.")
            else:
                try:
                    record_movement(sku, int(qty_final), reason)
                    novo_df_sku = stock_df(filter_text=sku)
                    try:
                        novo_estoque = int(novo_df_sku.loc[novo_df_sku["sku"] == sku, "estoque"].values[0])
                    except Exception:
                        novo_estoque = (estoque_atual or 0) + int(qty_final)
                    st.success(
                        f"Movimentação registrada: {sku} => {qty_final} ({reason}). "
                        f"Estoque: {estoque_atual} → **{novo_estoque}**."
                    )
                except Exception as e:
                    st.error(str(e))
    
    # -------- Baixa por PDF (usa processar_pdf_vendas) --------
    elif page == "Baixa por PDF":
        st.subheader("Baixa por PDF (layout UpSeller)")
        st.caption("Envie o PDF como o do UpSeller. O sistema identifica SKU e quantidade, mapeia e aplica as baixas.")
        
        up = st.file_uploader("Selecionar PDF", type=["pdf"])
        if up is not None:
            file_bytes = up.read()
            ok, movimentos, msg = processar_pdf_vendas(io.BytesIO(file_bytes))
            
            if not ok or not movimentos:
                st.error("Não foi possível identificar itens no PDF. Verifique o layout/arquivo.")
            else:
                st.success(msg)
                df_pdf = pd.DataFrame(movimentos)
                col_order = ["sku_pdf", "sku", "quantidade", "mapeado", "produto", "variacao"]
                df_pdf = df_pdf[[c for c in col_order if c in df_pdf.columns]]
                
                # Coluna editável para correção
                df_pdf["quantidade_corrigida"] = df_pdf["quantidade"].astype(int)
                
                st.write("Prévia (ajuste a coluna **Qtd. corrigida** se algum valor veio errado do PDF):")
                edited = st.data_editor(
                    df_pdf,
                    key="pdf_editor",
                    use_container_width=True,
                    num_rows="dynamic",
                    column_config={
                        "sku_pdf": st.column_config.TextColumn("SKU (PDF)", disabled=True),
                        "quantidade": st.column_config.NumberColumn("Qtd. lida (PDF)", disabled=True),
                        "quantidade_corrigida": st.column_config.NumberColumn(
                            "Qtd. corrigida",
                            min_value=1,
                            max_value=999,
                            step=1,
                            help="Altere aqui se a leitura do PDF veio com um zero a mais, etc."
                        ),
                        "sku": st.column_config.TextColumn("SKU (no estoque)"),
                        "mapeado": st.column_config.CheckboxColumn("Mapeado?", disabled=True),
                        "produto": st.column_config.TextColumn("Produto (PDF)", disabled=True),
                        "variacao": st.column_config.TextColumn("Variação (PDF)", disabled=True),
                    }
                )
                
                # Checagem de quantidades altas
                HIGH_QTY_THRESHOLD = 99
                
                # Conferência + Simulação
                st.markdown("### Conferência: Itens do PDF vs Estoque Atual")
                
                # Mapas auxiliares
                sku_san_to_orig = sanitized_to_original_sku_map()
                existentes = set(sku_san_to_orig.keys())
                df_estoque_atual = stock_df()
                map_estoque = {str(row["sku"]): int(row["estoque"]) for _, row in df_estoque_atual.iterrows()} if not df_estoque_atual.empty else {}
                
                preview = edited.copy()
                
                def to_original_if_possible(sku_val: str) -> str:
                    s = str(sku_val or "")
                    s_san = sanitize_sku(s)
                    return sku_san_to_orig.get(s_san, s)
                
                preview["SKU (PDF)"] = preview.get("sku_pdf", "")
                preview["SKU (no estoque)"] = preview.get("sku", "").map(to_original_if_possible)
                preview["Qtd. (PDF)"] = preview.get("quantidade", 0).astype(int)
                
                # usar sempre a corrigida
                qtd_usada = preview.get("quantidade_corrigida")
                if qtd_usada is None:
                    qtd_usada = preview["Qtd. (PDF)"]
                preview["Qtd. (usada)"] = pd.to_numeric(qtd_usada, errors="coerce").fillna(0).astype(int).clip(lower=0)
                
                preview["Estoque atual (antes)"] = (
                    preview["SKU (no estoque)"].map(lambda s: map_estoque.get(str(s), 0)).fillna(0).astype(int)
                )
                
                preview["Estoque após (simulado)"] = preview["Estoque atual (antes)"] - preview["Qtd. (usada)"]
                
                # Status textual
                def status_row(after: int) -> str:
                    if after < 0:
                        return "FICA NEGATIVO"
                    if after == 0:
                        return "ZERA ESTOQUE"
                    return "OK"
                
                preview["Status"] = preview["Estoque após (simulado)"].apply(status_row)
                preview["Qtd muito alta?"] = preview["Qtd. (usada)"] > HIGH_QTY_THRESHOLD
                
                cols_preview = [
                    "SKU (PDF)", "SKU (no estoque)", "Qtd. (PDF)", "Qtd. (usada)",
                    "Estoque atual (antes)", "Estoque após (simulado)", "Status", "Qtd muito alta?"
                ]
                preview = preview[cols_preview]
                
                # Destaques visuais
                def hl_simulado(row):
                    styles = [""] * len(row)
                    after = row.get("Estoque após (simulado)", 0)
                    if after < 0:
                        styles = ["background-color: #ffcccc"] * len(row)
                    elif after == 0:
                        styles = ["background-color: #fff2cc"] * len(row)
                    if bool(row.get("Qtd muito alta?", False)):
                        styles = ["background-color: #ffe5b4"] * len(row)
                    return styles
                
                show_only_critical = st.toggle("Mostrar apenas itens que zeram/ficam negativos", value=False)
                filtered_preview = preview.copy()
                if show_only_critical:
                    mask_crit = filtered_preview["Estoque após (simulado)"] <= 0
                    filtered_preview = filtered_preview[mask_crit]
                    st.caption(f"Exibindo {len(filtered_preview)} de {len(preview)} itens (apenas críticos).")
                
                st.dataframe(filtered_preview.style.apply(hl_simulado, axis=1), use_container_width=True)
                
                # Bloco de conferência de quantidades altas
                df_high = preview[preview["Qtd muito alta?"]].copy()
                confirm_high_needed = not df_high.empty
                confirm_high = False
                
                if confirm_high_needed:
                    st.warning(
                        f"⚠️ Encontramos {len(df_high)} linha(s) com quantidade acima de {HIGH_QTY_THRESHOLD}. "
                        "Confira os itens abaixo; corrija se necessário ou marque a confirmação para continuar."
                    )
                    st.dataframe(df_high, use_container_width=True)
                    confirm_high = st.checkbox(f"Confirmo as quantidades altas (>{HIGH_QTY_THRESHOLD}) apresentadas acima")
                
                # Botão: Simular baixa
                if st.button("🧪 Simular baixa (não grava)"):
                    if (edited.get("quantidade_corrigida", 0) <= 0).any():
                        st.error("Há linhas com 'Qtd. corrigida' inválida (<= 0). Corrija antes de simular.")
                    else:
                        total_itens = len(preview)
                        vai_negativo = int((preview["Estoque após (simulado)"] < 0).sum())
                        vai_zerar = int((preview["Estoque após (simulado)"] == 0).sum())
                        total_qtd = int(preview["Qtd. (usada)"].sum())
                        st.info(
                            f"Simulação: {total_itens} linhas | Total de peças (usadas): {total_qtd} | "
                            f"Zera estoque: {vai_zerar} | Fica negativo: {vai_negativo}"
                        )
                
                grava_map = st.checkbox("Salvar/atualizar mapeamentos sku_pdf → sku (para os itens com SKU preenchido)", value=True)
                
                # Botão: Aplicar baixas
                if st.button("Aplicar baixas (venda_pdf)", type="primary"):
                    if confirm_high_needed and not confirm_high:
                        st.error(f"Existem quantidades acima de {HIGH_QTY_THRESHOLD} não confirmadas. Confirme ou corrija antes de aplicar.")
                        st.stop()
                    
                    if edited.empty:
                        st.error("Não há itens para processar.")
                        st.stop()
                    
                    if (edited.get("quantidade_corrigida", 0) <= 0).any():
                        st.error("Há linhas com 'Qtd. corrigida' inválida (<= 0). Corrija antes de aplicar.")
                        st.stop()
                    
                    backup_database()
                    
                    ok_count = 0
                    mapeados = 0
                    erros = 0
                    faltando = 0
                    
                    for _, r in edited.iterrows():
                        sku_pdf = sanitize_sku(str(r.get("sku_pdf", "")))
                        qtd = int(r.get("quantidade_corrigida", r.get("quantidade", 0)) or 0)
                        sku_user = str(r.get("sku", ""))
                        sku_est_sanit = sanitize_sku(sku_user)
                        
                        if not sku_est_sanit:
                            faltando += 1
                            continue
                        
                        if sku_est_sanit not in existentes:
                            erros += 1
                            continue
                        
                        sku_original = sku_san_to_orig[sku_est_sanit]
                        
                        try:
                            record_movement(sku_original, -abs(qtd), "venda_pdf")
                            ok_count += 1
                            
                            if grava_map and sku_pdf:
                                try:
                                    with get_conn() as conn:
                                        conn.execute(
                                            text("""
                                                INSERT INTO sku_mapping (sku_pdf, sku_estoque) 
                                                VALUES (:sku_pdf, :sku_estoque)
                                                ON CONFLICT (sku_pdf) 
                                                DO UPDATE SET sku_estoque = EXCLUDED.sku_estoque
                                            """),
                                            {"sku_pdf": sku_pdf, "sku_estoque": sku_original}
                                        )
                                        conn.commit()
                                    mapeados += 1
                                except Exception:
                                    pass
                        except Exception:
                            erros += 1
                    
                    st.success(
                        f"Baixas aplicadas! OK: {ok_count} | Mapeamentos salvos: {mapeados} | "
                        f"Sem SKU preenchido: {faltando} | Erros: {erros}"
                    )
                
                st.divider()
                st.download_button(
                    "📥 Exportar leitura do PDF (CSV)",
                    edited.to_csv(index=False).encode("utf-8"),
                    "baixa_pdf_preview.csv",
                    "text/csv"
                )
    
    # -------- Estoque Atual --------
    elif page == "Estoque Atual":
        st.subheader("Estoque atual por SKU")
        f1, f2, f3 = st.columns([2,1,1])
        with f1:
            filtro = st.text_input("Filtro (SKU, categoria, subtipo, cor ou tamanho)")
        with f2:
            critico = st.number_input("Estoque crítico (abaixo de)", min_value=0, value=5, step=1)
        with f3:
            modo_exibicao = st.radio("Modo de exibição", ["Todos os itens", "Apenas críticos"], horizontal=True)
        
        apenas_criticos = (modo_exibicao == "Apenas críticos")
        df = stock_df(filter_text=filtro if filtro else None, critical_only=apenas_criticos, critical_value=critico)
        
        if not df.empty and 'valor_estoque' in df.columns:
            valor_total_estoque = df['valor_estoque'].sum()
            total_itens = len(df)
            total_unidades = df['estoque'].sum()
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total de itens", total_itens)
            with col2:
                st.metric("Total de unidades", total_unidades)
            with col3:
                st.metric("Valor total do estoque", f"R$ {valor_total_estoque:,.2f}")
            with col4:
                custo_medio = valor_total_estoque / total_unidades if total_unidades > 0 else 0
                st.metric("Custo médio por unidade", f"R$ {custo_medio:.2f}")
        
        if df.empty:
            st.info("Nenhuma variante encontrada.")
        else:
            def highlight(row):
                if row["estoque"] < 0:
                    return ["background-color: #ffcccc" for _ in row]
                if row["estoque"] <= critico:
                    return ["background-color: #fff2cc" for _ in row]
                return ["" for _ in row]
            
            display_df = df.copy()
            if 'custo_unitario' in display_df.columns:
                display_df['custo_unitario'] = display_df['custo_unitario'].apply(lambda x: f"R$ {x:,.2f}" if pd.notnull(x) else "R$ 0,00")
            if 'valor_estoque' in display_df.columns:
                display_df['valor_estoque'] = display_df['valor_estoque'].apply(lambda x: f"R$ {x:,.2f}" if pd.notnull(x) else "R$ 0,00")
            
            st.dataframe(display_df.style.apply(highlight, axis=1), use_container_width=True, hide_index=True)
            
            total_criticos = len(df[df["estoque"] <= critico])
            total_negativos = len(df[df["estoque"] < 0])
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Itens críticos", total_criticos)
            with col2:
                st.metric("Estoques negativos", total_negativos)
    
    # -------- Histórico --------
    elif page == "Histórico":
        st.subheader("Histórico de Movimentações")
        colf1, colf2, colf3 = st.columns([2,1,1])
        with colf1:
            vdf = list_variants_df()
            sku_options = [""] + vdf["sku"].tolist() if not vdf.empty else [""]
            sku_escolhido = st.selectbox("Filtrar por SKU (digite para filtrar)", sku_options, index=0)
        with colf2:
            motivo = st.selectbox("Motivo", ["Todos", "entrada", "venda", "venda_pdf", "ajuste"])
        with colf3:
            dias = st.selectbox("Período", ["Todos", "7", "30", "90"], index=2)
        
        days = None if dias == "Todos" else int(dias)
        dfh = movements_df(sku_filter=sku_escolhido if sku_escolhido else None, reason=motivo if motivo != "Todos" else None, days=days)
        st.dataframe(dfh, use_container_width=True)
    
    # -------- Exportar CSV --------
    elif page == "Exportar CSV":
        st.subheader("Exportar dados")
        v = list_variants_df()
        s = stock_df()
        m = movements_df()
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.download_button("📥 Variantes (CSV)", v.to_csv(index=False).encode("utf-8"), "variantes.csv", "text/csv")
        with col2:
            st.download_button("📥 Estoque (CSV)", s.to_csv(index=False).encode("utf-8"), "estoque.csv", "text/csv")
        with col3:
            st.download_button("📥 Movimentações (CSV)", m.to_csv(index=False).encode("utf-8"), "movimentacoes.csv", "text/csv")
    
    # -------- Editar Variante --------
    elif page == "Editar Variante":
        st.subheader("Editar Variante (com autocomplete de SKU)")
        vdf = list_variants_df()
        current_sku = st.selectbox("Selecione o SKU", vdf["sku"].tolist() if not vdf.empty else [], index=None, placeholder="Digite parte do SKU…")
        
        if current_sku:
            det = get_variant_details(current_sku)
            if det:
                col1, col2, col3, col4, col5 = st.columns(5)
                with col1:
                    category = st.text_input("Categoria", det["category"])
                with col2:
                    subtype = st.text_input("Subtipo", det["subtype"])
                with col3:
                    color = st.text_input("Cor", det["color"])
                with col4:
                    size = st.text_input("Tamanho", det["size"])
                with col5:
                    sku_base = st.text_input("SKU Base", det["sku_base"] or "")
                
                c1, c2 = st.columns(2)
                with c1:
                    custo_unitario_produto = st.number_input("Custo Unitário (PRODUTO) R$", min_value=0.0, value=float(det.get("custo_unitario_produto", 0) or 0), step=0.01, help="Custo padrão do tipo/subtipo. Variantes podem ter custo próprio.")
                with c2:
                    cur_val = det.get("custo_unitario_variante", None)
                    custo_unitario_variante = st.number_input("Custo Unitário (VARIANTE) R$ (opcional)", min_value=0.0, value=float(cur_val if cur_val is not None else 0.0), step=0.01, help="Se > 0, substitui o custo do produto apenas para esta variante.")
                
                new_sku = st.text_input("Novo SKU", det["sku"])
                
                if st.button("Salvar alterações", type="primary"):
                    cvar = custo_unitario_variante if custo_unitario_variante > 0 else None
                    ok, msg = update_variant(det["sku"], new_sku, category, subtype, color, size, sku_base if sku_base else None, custo_unitario_produto, cvar)
                    if ok:
                        st.success(msg)
                    else:
                        st.error(msg)
            else:
                st.error("SKU não encontrado.")
    
    # -------- Remover Variante --------
    elif page == "Remover Variante":
        st.subheader("Remover Variante")
        vdf = list_variants_df()
        sku = st.selectbox("Selecione o SKU", vdf["sku"].tolist() if not vdf.empty else [], index=None, placeholder="Digite para filtrar…")
        
        if st.button("Remover", type="primary"):
            if not sku:
                st.error("Selecione um SKU.")
            else:
                ok, msg = delete_variant(sku)
                if ok:
                    st.success(msg)
                else:
                    st.error(msg)
    
    # -------- Mapeamento de SKUs --------
    elif page == "Mapeamento de SKUs":
        st.subheader("Mapeamentos (sku_pdf → sku)")
        
        df_map = execute_sql("SELECT id, sku_pdf, sku_estoque FROM sku_mapping ORDER BY id DESC")
        if df_map is not None and not df_map.empty:
            st.dataframe(df_map, use_container_width=True)
        else:
            st.info("Nenhum mapeamento encontrado.")
        
        # Excluir mapeamento
        st.markdown("### Excluir mapeamento existente")
        if df_map is not None and not df_map.empty:
            col_del1, col_del2, col_del3 = st.columns([2,2,1])
            with col_del1:
                del_by = st.radio("Selecionar por", ["ID", "SKU (PDF)"], horizontal=True)
            with col_del2:
                if del_by == "ID":
                    sel_id = st.selectbox("ID do mapeamento", df_map["id"].tolist(), index=None, placeholder="Selecione o ID…")
                    sel_sku_pdf = None
                else:
                    sel_sku_pdf = st.selectbox("SKU (PDF)", df_map["sku_pdf"].tolist(), index=None, placeholder="Selecione o SKU (PDF)…")
                    sel_id = None
            with col_del3:
                do_delete = st.button("🗑️ Excluir", type="secondary")
            
            if do_delete:
                try:
                    with get_conn() as conn:
                        if del_by == "ID" and sel_id is not None:
                            conn.execute(text("DELETE FROM sku_mapping WHERE id = :id"), {"id": int(sel_id)})
                            conn.commit()
                            st.success(f"Mapeamento ID {sel_id} excluído.")
                            st.rerun()
                        elif del_by != "ID" and sel_sku_pdf:
                            conn.execute(text("DELETE FROM sku_mapping WHERE sku_pdf = :sku_pdf"), {"sku_pdf": str(sel_sku_pdf)})
                            conn.commit()
                            st.success(f"Mapeamento do SKU (PDF) '{sel_sku_pdf}' excluído.")
                            st.rerun()
                        else:
                            st.warning("Selecione um item para excluir.")
                except Exception as e:
                    st.error(f"Erro ao excluir mapeamento: {e}")
        else:
            st.info("Não há mapeamentos para excluir.")
        
        # Adicionar mapeamento
        with st.expander("Adicionar mapeamento manualmente"):
            col1, col2 = st.columns(2)
            with col1:
                sku_pdf = st.text_input("SKU (PDF)")
            with col2:
                vdf = list_variants_df()
                sku_options = vdf["sku"].tolist() if not vdf.empty else []
                sku_estoque = st.selectbox("SKU no estoque", sku_options, index=None, placeholder="Digite para filtrar…")
            
            if st.button("Adicionar mapeamento"):
                if sku_pdf and sku_estoque:
                    try:
                        with get_conn() as conn:
                            conn.execute(
                                text("""
                                    INSERT INTO sku_mapping (sku_pdf, sku_estoque) 
                                    VALUES (:sku_pdf, :sku_estoque)
                                    ON CONFLICT (sku_pdf) 
                                    DO UPDATE SET sku_estoque = EXCLUDED.sku_estoque
                                """),
                                {"sku_pdf": sanitize_sku(sku_pdf), "sku_estoque": str(sku_estoque)}
                            )
                            conn.commit()
                        st.success("Mapeamento adicionado/atualizado.")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))
                else:
                    st.error("Preencha os dois campos.")
    
    # -------- Gerenciar SKU Base --------
    elif page == "Gerenciar SKU Base":
        st.subheader("Atualizar SKU Base e regenerar SKUs das variantes")
        col1, col2, col3 = st.columns(3)
        with col1:
            category = st.text_input("Categoria")
        with col2:
            subtype = st.text_input("Subtipo")
        with col3:
            new_base = st.text_input("Novo SKU Base (ex.: MOL-CARECA)")
        
        if st.button("Atualizar SKU Base", type="primary"):
            if not (category and subtype and new_base):
                st.error("Preencha categoria, subtipo e novo SKU base.")
            else:
                ok, msg = update_sku_base_bulk(category, subtype, new_base)
                if ok:
                    st.success(msg)
                else:
                    st.error(msg)
    
    # -------- Custo por Categoria/Subtipo (em massa) --------
    elif page == "Custo por Categoria/Subtipo (em massa)":
        st.subheader("Atualizar Custo Unitário em Massa por Categoria/Subtipo")
        df_produtos = list_products_df()
        
        if df_produtos.empty:
            st.info("Nenhum produto cadastrado ainda.")
        else:
            categorias = sorted(df_produtos["category"].dropna().unique().tolist())
            c1, c2 = st.columns([2, 3])
            with c1:
                categoria_escolhida = st.selectbox("Categoria", [""] + categorias, index=0)
            with c2:
                if categoria_escolhida:
                    subtipos_disp = (
                        df_produtos[df_produtos["category"] == categoria_escolhida]["subtype"]
                        .dropna().unique().tolist()
                    )
                    subtipos_disp = sorted(subtipos_disp)
                    subtipos_escolhidos = st.multiselect(
                        "Subtipos (se vazio, aplica em TODOS os subtipos da categoria)",
                        subtipos_disp,
                        default=subtipos_disp
                    )
                else:
                    subtipos_escolhidos = []
            
            novo_custo = st.number_input(
                "Novo Custo Unitário (PRODUTO) R$",
                min_value=0.0,
                value=0.0,
                step=0.01,
                help="Atualiza o custo padrão do produto. Variantes com custo próprio não são afetadas."
            )
            
            afetadas = 0
            if categoria_escolhida:
                df_alvo = df_produtos[df_produtos["category"] == categoria_escolhida]
                if subtipos_escolhidos:
                    df_alvo = df_alvo[df_alvo["subtype"].isin(subtipos_escolhidos)]
                vdf = list_variants_df()
                prod_ids = df_alvo["id"].tolist()
                afetadas = len(vdf[vdf["product_id"].isin(prod_ids)]) if "product_id" in vdf.columns else 0
                st.caption(f"Variantes impactadas (estimativa): **{afetadas}** (apenas no custo padrão; variantes com custo próprio continuam com o seu valor)")
            
            colb1, colb2 = st.columns([1, 2])
            with colb1:
                aplicar = st.button("Aplicar custo em massa", type="primary")
            
            if aplicar:
                if not categoria_escolhida:
                    st.error("Escolha uma categoria.")
                elif novo_custo <= 0:
                    st.error("Informe um custo maior que zero.")
                else:
                    alvo = df_produtos[df_produtos["category"] == categoria_escolhida]
                    if subtipos_escolhidos:
                        alvo = alvo[alvo["subtype"].isin(subtipos_escolhidos)]
                    
                    if alvo.empty:
                        st.warning("Não há produtos para atualizar com os filtros escolhidos.")
                    else:
                        ok_cnt, err_cnt = 0, 0
                        for _, row in alvo.iterrows():
                            ok, msg = update_custo_unitario(row["category"], row["subtype"], float(novo_custo))
                            if ok:
                                ok_cnt += 1
                            else:
                                err_cnt += 1
                                st.warning(f"{row['category']} / {row['subtype']}: {msg}")
                        
                        st.success(f"Custo padrão atualizado para R$ {novo_custo:.2f} em {ok_cnt} produto(s).")
                        if err_cnt:
                            st.error(f"Ocorreu erro em {err_cnt} produto(s).")
    
    # -------- Contagem de Estoque --------
    elif page == "Contagem de Estoque":
        st.subheader("Contagem de Estoque (ajuste por inventário)")
        vdf = list_variants_df()
        sku = st.selectbox("SKU", vdf["sku"].tolist() if not vdf.empty else [], index=None, placeholder="Digite para filtrar…")
        
        if sku:
            atual = stock_df(filter_text=sku)
            saldo_atual = int(atual.loc[atual["sku"] == sku, "estoque"].values[0]) if not atual.empty else 0
            novo = st.number_input("Quantidade contada (substitui o saldo)", value=saldo_atual, step=1)
            
            if st.button("Aplicar contagem", type="primary"):
                delta = novo - saldo_atual
                if delta != 0:
                    record_movement(sku, int(delta), "ajuste")
                    st.success(f"Saldo ajustado. Anterior: {saldo_atual} | Novo: {novo}")
    
    # -------- Valor do Estoque --------
    elif page == "Valor do Estoque":
        st.subheader("💰 Valor Total do Estoque")
        col1, col2 = st.columns(2)
        with col1:
            filtro_categoria = st.text_input("Filtrar por categoria", placeholder="Ex: moletom, camiseta")
        with col2:
            filtro_subtipo = st.text_input("Filtrar por subtipo", placeholder="Ex: canguru, careca")
        
        df_estoque = stock_value_df()
        if filtro_categoria:
            df_estoque = df_estoque[df_estoque['category'].str.contains(filtro_categoria, case=False, na=False)]
        if filtro_subtipo:
            df_estoque = df_estoque[df_estoque['subtype'].str.contains(filtro_subtipo, case=False, na=False)]
        
        if df_estoque.empty:
            st.info("Nenhum item encontrado com os filtros aplicados.")
        else:
            valor_total = df_estoque['valor_estoque'].sum()
            total_itens = len(df_estoque)
            total_unidades = df_estoque['estoque'].sum()
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Valor Total do Estoque", f"R$ {valor_total:,.2f}")
            with col2:
                st.metric("Total de Itens", total_itens)
            with col3:
                st.metric("Total de Unidades", total_unidades)
            with col4:
                custo_medio = valor_total / total_unidades if total_unidades > 0 else 0
                st.metric("Custo Médio por Unidade", f"R$ {custo_medio:.2f}")
            
            st.divider()
            st.subheader("Valor por Categoria/Subtipo")
            df_agrupado = df_estoque.groupby(['category', 'subtype']).agg({
                'estoque': 'sum',
                'valor_estoque': 'sum',
                'sku': 'count'
            }).reset_index().rename(columns={'sku':'quantidade_skus','estoque':'total_unidades'}).sort_values('valor_estoque', ascending=False)
            
            disp = df_agrupado.copy()
            disp['valor_estoque'] = disp['valor_estoque'].apply(lambda x: f"R$ {x:,.2f}")
            st.dataframe(disp, use_container_width=True)
            
            st.divider()
            st.subheader("Detalhamento Completo do Estoque")
            detalhado = df_estoque.copy()
            detalhado['custo_unitario'] = detalhado['custo_unitario'].apply(lambda x: f"R$ {x:,.2f}")
            detalhado['valor_estoque'] = detalhado['valor_estoque'].apply(lambda x: f"R$ {x:,.2f}")
            st.dataframe(detalhado, use_container_width=True)
            
            csv = df_estoque.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Exportar Dados de Valor do Estoque (CSV)", csv, "valor_estoque.csv", "text/csv")
    
    # -------- Gráfico de Vendas --------
    elif page == "Gráfico de Vendas":
        st.subheader("📊 Gráfico de Vendas")
        coltop1, coltop2, coltop3 = st.columns(3)
        with coltop1:
            periodo = st.selectbox(
                "Período",
                ["Últimos 7 dias", "Últimos 30 dias", "Últimos 90 dias", "Todo o período"],
                index=1
            )
        with coltop2:
            limite_produtos = st.slider("Nº no ranking de produtos", 5, 30, 10)
        with coltop3:
            modo_valor = st.selectbox("Métrica financeira", ["Valor ao Custo", "Somente Quantidade"], index=0)
        
        dias_map = {"Últimos 7 dias": 7, "Últimos 30 dias": 30, "Últimos 90 dias": 90, "Todo o período": None}
        dias = dias_map[periodo]
        df_vendas = get_sales_data(days=dias)
        
        if df_vendas.empty:
            st.info("Nenhuma venda no período.")
        else:
            st.markdown("### Filtros por Produto")
            f1, f2 = st.columns(2)
            with f1:
                filtro_cat = st.text_input("Categoria (ex.: MOLETOM, CAMISETA)", value="")
            with f2:
                filtro_sub = st.text_input("Subtipo (ex.: CARECA, CANGURU)", value="")
            
            df_prod = df_vendas.copy()
            if filtro_cat:
                df_prod = df_prod[df_prod["category"].str.contains(filtro_cat, case=False, na=False)]
            if filtro_sub:
                df_prod = df_prod[df_prod["subtype"].str.contains(filtro_sub, case=False, na=False)]
            
            total_qtd = int(df_prod["quantidade_vendida"].sum())
            total_val = float(df_prod["valor_total_vendido"].sum())
            total_regs = int(df_prod["numero_vendas"].sum())
            
            m1, m2, m3 = st.columns(3)
            with m1:
                st.metric("Unidades vendidas", total_qtd)
            with m2:
                st.metric("Valor ao custo", f"R$ {total_val:,.2f}")
            with m3:
                st.metric("Registros (linhas) de venda", total_regs)
            
            st.divider()
            st.markdown("### Top Produtos (Categoria-Subtipo)")
            df_top = (
                df_prod.groupby(["category", "subtype"], as_index=False)[["quantidade_vendida", "valor_total_vendido"]]
                .sum()
                .assign(produto=lambda d: d["category"] + " - " + d["subtype"])
                .sort_values("quantidade_vendida", ascending=False)
                .head(limite_produtos)
            )
            
            if not df_top.empty:
                fig1 = px.bar(
                    df_top,
                    x="quantidade_vendida",
                    y="produto",
                    orientation="h",
                    title=f"Top {limite_produtos} por Quantidade",
                    labels={"quantidade_vendida":"Quantidade","produto":"Produto"}
                )
                fig1.update_layout(yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig1, use_container_width=True)
                
                if modo_valor == "Valor ao Custo":
                    fig2 = px.bar(
                        df_top.sort_values("valor_total_vendido"),
                        x="valor_total_vendido",
                        y="produto",
                        orientation="h",
                        title=f"Top {limite_produtos} por Valor (Custo)",
                        labels={"valor_total_vendido":"Valor (R$)","produto":"Produto"}
                    )
                    fig2.update_layout(yaxis={'categoryorder':'total ascending'})
                    st.plotly_chart(fig2, use_container_width=True)
            
            st.divider()
            st.markdown("### Tamanhos mais vendidos (no produto filtrado)")
            if not df_prod.empty:
                df_tam = (
                    df_prod.groupby("size", as_index=False)[["quantidade_vendida","valor_total_vendido"]]
                    .sum().sort_values("quantidade_vendida", ascending=False).head(30)
                )
                fig_tam = px.bar(
                    df_tam,
                    x="size",
                    y="quantidade_vendida",
                    title="Top Tamanhos (por quantidade) — respeitando filtros de Categoria/Subtipo",
                    labels={"size":"Tamanho","quantidade_vendida":"Quantidade"}
                )
                st.plotly_chart(fig_tam, use_container_width=True)
            else:
                st.info("Aplique filtros de Categoria/Subtipo para ver tamanhos específicos do produto.")
            
            st.divider()
            st.markdown("### Top Itens (Categoria-Subtipo-Cor-Tamanho)")
            df_itens = (
                df_prod
                .assign(
                    item=lambda d: (
                        d["category"].astype(str).str.upper().str.replace(r"\s+","-", regex=True) + "-" +
                        d["subtype"].astype(str).str.upper().str.replace(r"\s+","-", regex=True) + "-" +
                        d["color"].astype(str).str.upper().str.replace(r"\s+","-", regex=True) + "-" +
                        d["size"].astype(str).str.upper()
                    )
                )
                .groupby("item", as_index=False)[["quantidade_vendida","valor_total_vendido"]]
                .sum().sort_values("quantidade_vendida", ascending=False)
            )
            
            df_itens = df_itens.sort_values("quantidade_vendida", ascending=False)
            n_itens = st.slider("Quantos itens mostrar no ranking?", 5, 100, 20, key="slider_top_itens")
            top_itens = df_itens.head(n_itens)
            
            cti1, cti2 = st.columns(2)
            with cti1:
                fig_items_q = px.bar(
                    top_itens.sort_values("quantidade_vendida"),
                    x="quantidade_vendida",
                    y="item",
                    orientation="h",
                    title=f"Top {n_itens} Itens por Quantidade",
                    labels={"quantidade_vendida":"Quantidade","item":"Item"}
                )
                fig_items_q.update_layout(yaxis={"categoryorder": "total ascending"})
                st.plotly_chart(fig_items_q, use_container_width=True)
            
            with cti2:
                fig_items_v = px.bar(
                    top_itens.sort_values("valor_total_vendido"),
                    x="valor_total_vendido",
                    y="item",
                    orientation="h",
                    title=f"Top {n_itens} Itens por Valor (ao custo)",
                    labels={"valor_total_vendido":"Valor (R$)","item":"Item"}
                )
                fig_items_v.update_layout(yaxis={"categoryorder": "total ascending"})
                st.plotly_chart(fig_items_v, use_container_width=True)
            
            st.dataframe(top_itens, use_container_width=True)
            st.download_button(
                "📥 Exportar Top Itens (CSV)",
                df_itens.to_csv(index=False).encode("utf-8"),
                "ranking_top_itens.csv",
                "text/csv"
            )
            
            st.divider()
            if not filtro_cat and not filtro_sub:
                st.markdown("### Distribuição por Categoria (geral)")
                df_cat = (
                    df_vendas.groupby('category')
                    .agg({'quantidade_vendida':'sum','valor_total_vendido':'sum'})
                    .reset_index().sort_values('quantidade_vendida', ascending=False)
                )
                c1, c2 = st.columns(2)
                with c1:
                    st.plotly_chart(px.pie(df_cat, values='quantidade_vendida', names='category', title='Vendas por Categoria (Qtd)'), use_container_width=True)
                with c2:
                    st.plotly_chart(px.pie(df_cat, values='valor_total_vendido', names='category', title='Vendas por Categoria (Valor)'), use_container_width=True)
            
            st.divider()
            st.download_button(
                "📥 Exportar Dados de Vendas (CSV — filtros aplicados)",
                df_prod.to_csv(index=False).encode("utf-8"),
                f"vendas_{periodo.lower().replace(' ','_')}_filtrado.csv",
                "text/csv"
            )
    
    # -------- Migrar Dados --------
    elif page == "Migrar Dados":
        st.subheader("🚀 Migração de Dados")
        
        if is_postgres():
            st.success("✅ Conectado ao PostgreSQL")
            
            st.info("""
            **Migração SQLite → PostgreSQL**
            
            Esta função migra todos os dados do SQLite local para o PostgreSQL na nuvem.
            
            **O que será migrado:**
            - Produtos (products)
            - Variantes (variants) 
            - Movimentações (movements)
            - Mapeamentos (sku_mapping)
            """)
            
            if st.button("Iniciar Migração Completa", type="primary"):
                with st.spinner("Migrando dados..."):
                    migrate_from_sqlite_to_postgres()
        else:
            st.warning("⚠️ Conecte-se ao PostgreSQL primeiro para migrar dados")
            st.info("""
            **Para conectar ao PostgreSQL:**
            
            1. Crie uma conta no [Supabase](https://supabase.com)
            2. Crie um novo projeto
            3. Vá em **Settings > Database** e copie a Connection String
            4. No Streamlit Cloud, vá em **Settings → Secrets**
            5. Adicione:
            ```toml
            [connections.supabase]
            url = "sua_connection_string_aqui"
            ```
            6. Reinicie o app
            """)
    
    # -------- Rodapé --------
    st.divider()
    st.caption("© Controle de Estoque — feito com Streamlit + PostgreSQL/Supabase. Auditoria por movimentação e saldo por SKU.")

if __name__ == "__main__":
    main()