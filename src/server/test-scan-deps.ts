import { qualityBandForScore } from "@/lib/shared/scans";
import type { LighthouseRunResult } from "@/server/lighthouse";
import { streamOpenAiText } from "@/server/providers/openai/client";

const requestCounts = new Map<string, number>();

export function resetRequestCounts(): void {
  requestCounts.clear();
}

function createDefaultMockHtml(): string {
  return `
    <!DOCTYPE html>
    <html>
      <head>
        <title>Khan Academy Kids - Free Educational Games</title>
        <meta name="description" content="A free educational app with thousands of activities, books, and games for children ages 2-8.">
      </head>
      <body>
        <nav><a href="/about">About</a><a href="/parents">Parents</a><a href="/teachers">Teachers</a></nav>
        <h1>Learning Adventures for Curious Minds</h1>
        <section>Trusted by teachers. Loved by parents. Built for kids ages 2-8.</section>
        <section>Download the free app and start learning today.</section>
        <footer><a href="/privacy">Privacy</a></footer>
      </body>
    </html>
  `;
}

function createDifferentMockHtml(): string {
  return `
    <!DOCTYPE html>
    <html>
      <head>
        <title>Cyberr Talent Network</title>
        <meta name="description" content="Hire vetted cybersecurity experts for your team.">
      </head>
      <body>
        <nav><a href="/about">About</a><a href="/jobs">Jobs</a><a href="/talent">Talent</a></nav>
        <h1>Hire Cybersecurity Experts</h1>
        <section>Vetted professionals only. Ready to join your team.</section>
        <section>Apply today and join the network.</section>
        <footer><a href="/privacy">Privacy</a></footer>
      </body>
    </html>
  `;
}

function createMockFetch(): typeof fetch {
  return async (input) => {
    const url =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : input.url;

    const count = (requestCounts.get(url) ?? 0) + 1;
    requestCounts.set(url, count);

    // For example.org, return different HTML on the second request to simulate a site change
    // that falls below the 80% similarity threshold, forcing a fresh run.
    const html =
      url.includes("example.net") && count > 1
        ? createDifferentMockHtml()
        : createDefaultMockHtml();

    return new Response(html, {
      status: 200,
      headers: { "content-type": "text/html" },
    });
  };
}

function createMockLighthouse(): (targetUrl: string) => Promise<LighthouseRunResult> {
  return async () => {
    const mobileScore = 61;
    const desktopScore = 71;
    const qualityScore = Math.round((mobileScore + desktopScore) / 2);

    return {
      profiles: {
        mobile: {
          score: mobileScore,
          band: qualityBandForScore(mobileScore),
          snapshot: {
            performance: 55,
            accessibility: 72,
            bestPractices: 68,
            seo: 70,
            strategy: "mobile",
            source: "local",
          },
        },
        desktop: {
          score: desktopScore,
          band: qualityBandForScore(desktopScore),
          snapshot: {
            performance: 69,
            accessibility: 74,
            bestPractices: 70,
            seo: 71,
            strategy: "desktop",
            source: "local",
          },
        },
      },
      qualityScore,
      qualityBand: qualityBandForScore(qualityScore),
      raw: {
        mobile: { categories: {} },
        desktop: { categories: {} },
      },
      status: {
        provider: "lighthouse",
        source: "local",
        reason: "Local Lighthouse completed",
        latencyMs: 50,
      },
    };
  };
}

function createMockStreamText(): typeof streamOpenAiText {
  return async ({ system, onText }) => {
    const isJsonFindings = system.includes("Output valid JSON only");

    const text = isJsonFindings
      ? JSON.stringify({
          marketing: {
            title: "Learning outcome promise buried — age groups and subjects not visible above fold",
            fix: "Add age range and subject areas directly to the hero headline",
            severity: "HIGH",
          },
          seo: {
            title: "SEO score indicates meta descriptions missing educational keywords",
            fix: "Add learning outcomes, age range, and subject keywords to all page meta descriptions",
            severity: "MEDIUM",
          },
          performance: {
            title: "Performance score leaves mobile speed as conversion risk",
            fix: "Compress hero images and defer non-critical scripts to improve first paint",
            severity: "MEDIUM",
          },
        })
      : "Khan Academy Kids has a warm, playful offer for young learners, but the hero buries the specific age range and subjects. Parent trust is implied rather than explicit near the download CTA. Add educator endorsements or COPPA-safe badges closer to the action, and surface a screenshot or short demo so visitors know what the experience looks like before installing. tl;dr: tighten hero specificity, add trust signals near CTA, and show the product.";

    await onText(text);
    return {
      text,
      status: {
        provider: "openai" as const,
        source: "live" as const,
        reason: "Mock OpenAI response for tests",
        model: "gpt-5.4-nano",
        latencyMs: 10,
      },
    };
  };
}

export interface ScanRuntimeDependencies {
  fetchImpl?: typeof fetch;
  runLighthouse?: (targetUrl: string) => Promise<LighthouseRunResult>;
  streamText?: typeof streamOpenAiText;
}

export function buildTestScanDeps(): Partial<ScanRuntimeDependencies> {
  const shouldMock =
    process.env.SLC_MOCK_OPENAI === "true" || process.env.SLC_MOCK_SCAN === "true";

  if (!shouldMock) {
    return {};
  }

  return {
    fetchImpl: createMockFetch(),
    runLighthouse: createMockLighthouse(),
    streamText: createMockStreamText(),
  };
}
