const fetch = require('node-fetch');

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');

  try {
    // API optimizada que procesa las tasas P2P Binance USDT/VES en tiempo real
    const response = await fetch("https://criptoya.com/api/binancep2p/sell/usdt/ves/5", {
      headers: { "User-Agent": "Mozilla/5.0" }
    });
    
    const responseBuy = await fetch("https://criptoya.com/api/binancep2p/buy/usdt/ves/5", {
      headers: { "User-Agent": "Mozilla/5.0" }
    });

    const sellData = await response.json();
    const buyData = await responseBuy.json();

    // Formatear datos para la interfaz
    const buys = (sellData.data || []).map(item => ({
      adv: { price: item.price },
      advertiser: { nickName: item.userName || "Comerciante" }
    }));

    const sells = (buyData.data || []).map(item => ({
      adv: { price: item.price },
      advertiser: { nickName: item.userName || "Comerciante" }
    }));

    return res.status(200).json({ status: "success", buy: buys, sell: sells });
  } catch (error) {
    return res.status(500).json({ status: "error", message: error.message });
  }
};
