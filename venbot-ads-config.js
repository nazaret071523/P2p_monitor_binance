/*
 * VENBOT AI · PUBLICIDAD WEB
 * Fase 18 · v31.72
 *
 * Esta configuración está preparada para Google AdSense.
 * NO colocar aquí datos ficticios. Mantener publisherId/slots vacíos
 * hasta disponer de los valores reales de la cuenta aprobada.
 *
 * Flujo:
 * 1) Crear/validar la cuenta AdSense.
 * 2) Añadir el sitio Venbot y completar su revisión.
 * 3) Crear unidades de anuncio Display para header/top/footer.
 * 4) Pegar aquí el publisherId y los IDs de cada slot.
 * 5) Si Google exige consentimiento para una región, configurar
 *    Privacy & messaging/CMP en AdSense antes de servir anuncios
 *    personalizados en esa región.
 */
window.VENBOT_AD_CONFIG = {
  webEnabled: true,
  provider: "adsense",
  publisherId: "",
  slots: {
    header: "",
    top: "",
    footer: ""
  },
  consentRequired: false,
  consentStorageKey: "venbot-ads-consent",
  appReady: true
};
