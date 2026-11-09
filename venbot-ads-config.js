/*
 * VENBOT AI · Google AdSense web integration
 * Release candidate 31.73.43
 *
 * Intentionally fail-closed. Do not enable until the AdSense site is approved,
 * a Google-certified CMP/TCF is installed where required, and each ad unit has
 * a real slot ID copied from the owner's AdSense account.
 */
window.VENBOT_AD_CONFIG = {
  webEnabled: true,
  provider: "adsense",
  publisherId: "ca-pub-1110097120437683",
  siteApproved: false,
  cmpConfigured: false,
  consentRequired: true,
  consentStorageKey: "venbot-ads-consent",
  slots: {
    header: "",
    top: "",
    footer: ""
  }
};
