def consultar_binance_top1(trade_type, monto):
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    payload = json.dumps({
        "asset": "USDT",
        "fiat": "VES",
        "merchantCheck": False,
        "publisherType": "user",  # Filtro nativo de Binance para solo usuarios NO verificados
        "page": 1,
        "rows": 10,
        "tradeType": trade_type,
        "transAmount": str(monto)
    }).encode('utf-8')
    
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    
    try:
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=6) as response:
            res = json.loads(response.read().decode('utf-8'))
            data = res.get('data', [])
            
            # Toma el primer anuncio devuelto (Top 1 no verificado)
            if data:
                return round(float(data[0]['adv']['price']), 2)
    except Exception as e:
        print(f"Error consultando Binance Top1 ({trade_type}): {e}")
    return None

def get_p2p_rates():
    # SELL: Anuncios de venta de USDT = Tu precio para RECOMPRAR USDT a 10K VES
    tasa_recompra = consultar_binance_top1("SELL", "10000")
    
    # BUY: Anuncios de compra de USDT = Tu precio para VENDER USDT a 300K VES
    tasa_venta = consultar_binance_top1("BUY", "300000")
    
    if not tasa_recompra or not tasa_venta:
        return None, None, None, None

    spread = round(tasa_venta - tasa_recompra, 2)
    pct_bruto = round((spread / tasa_recompra) * 100, 2)
    return tasa_recompra, tasa_venta, spread, pct_bruto
