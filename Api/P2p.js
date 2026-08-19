const fetch = require('node-fetch');

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');

  const fetchTrade = async (type) => {
    const url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search";
    const payload = { asset: "USDT", fiat: "VES", merchantCheck: false, page: 1, rows: 5, tradeType: type };
    
    try {
      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const data = await response.json();
      return data.data || [];
    } catch (err) {
      return [];
    }
  };

  try {
    const [buys, sells] = await Promise.all([fetchTrade("BUY"), fetchTrade("SELL")]);
    return res.status(200).json({ status: "success", buy: buys, sell: sells, timestamp: new Date().toISOString() });
  } catch (error) {
    return res.status(500).json({ status: "error", message: "Error al conectar con Binance" });
  }
}
