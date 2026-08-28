import os
import psycopg2
from datetime import datetime, timedelta

# Obtenemos la URL de conexión desde las variables de entorno de Render
DATABASE_URL = os.getenv("DATABASE_URL")

def registrar_pago_db(telegram_id: int, username: str, referencia: str) -> bool:
    """Registra o actualiza el pago de un usuario en estado pendiente."""
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO usuarios_p2p (telegram_id, username, estado_suscripcion, referencia_pago)
                    VALUES (%s, %s, 'pendiente', %s)
                    ON CONFLICT (telegram_id) 
                    DO UPDATE SET referencia_pago = %s, estado_suscripcion = 'pendiente';
                """, (telegram_id, username, referencia, referencia))
                conn.commit()
        return True
    except Exception as e:
        print(f"Error en registrar_pago_db: {e}")
        return False

def verificar_estado_usuario(telegram_id: int) -> dict:
    """Verifica el estado actual de la suscripción del usuario."""
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT estado_suscripcion, fecha_expiracion, referencia_pago 
                    FROM usuarios_p2p WHERE telegram_id = %s;
                """, (telegram_id,))
                row = cur.fetchone()
                if row:
                    return {
                        "estado": row[0],
                        "expiracion": row[1],
                        "referencia": row[2]
                    }
        return {"estado": "no_registrado"}
    except Exception as e:
        print(f"Error en verificar_estado_usuario: {e}")
        return {"estado": "error"}

def aprobar_usuario_db(telegram_id: int) -> bool:
    """Aprueba la suscripción por 30 días (ideal para tu comando de admin)."""
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE usuarios_p2p 
                    SET estado_suscripcion = 'activo', 
                        fecha_expiracion = CURRENT_TIMESTAMP + INTERVAL '30 days'
                    WHERE telegram_id = %s;
                """, (telegram_id,))
                conn.commit()
        return True
    except Exception as e:
        print(f"Error en aprobar_usuario_db: {e}")
        return False
