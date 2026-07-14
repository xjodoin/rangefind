// Per-page JSON-LD structured data: WebSite on every page,
// SoftwareApplication on the landing page, TechArticle on docs pages.
const prefix = (process.env.PATH_PREFIX || "/").replace(/\/$/, "");

export default {
  jsonLd: data => {
    const origin = data.site.origin;
    const pageUrl = `${origin}${prefix}${data.page.url}`;
    const graph = [
      {
        "@type": "WebSite",
        "@id": `${origin}${prefix}/#website`,
        name: "Rangefind",
        url: `${origin}${prefix}/`,
        description: data.site.description
      }
    ];
    if (data.page.url === "/") {
      graph.push({
        "@type": "SoftwareApplication",
        name: "Rangefind",
        url: `${origin}${prefix}/`,
        description: data.site.description,
        applicationCategory: "DeveloperApplication",
        operatingSystem: "Any",
        license: "https://opensource.org/license/mit/",
        offers: { "@type": "Offer", price: "0", priceCurrency: "USD" },
        codeRepository: data.site.repo,
        author: { "@type": "Person", name: "Xavier Jodoin", url: data.site.repo.replace(/\/rangefind$/, "") }
      });
    } else if (data.page.url.startsWith("/docs/") || data.page.url === "/changelog/") {
      graph.push({
        "@type": "TechArticle",
        headline: data.title || "Rangefind documentation",
        description: data.description || data.site.description,
        url: pageUrl,
        isPartOf: { "@id": `${origin}${prefix}/#website` },
        author: { "@type": "Person", name: "Xavier Jodoin" }
      });
    }
    return JSON.stringify({ "@context": "https://schema.org", "@graph": graph });
  }
};
