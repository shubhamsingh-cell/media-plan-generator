# Nova AI Suite -- Environment Variables Reference

## Authentication (2 vars)
| Variable | Required | Description |
|----------|----------|-------------|
| NOVA_ADMIN_KEY | No | Admin API key for /api/admin/* endpoints |
| NOVA_API_KEYS | No | Comma-separated API keys for /api/* access |

## LLM Providers (12 vars)
| Variable | Required | Provider |
|----------|----------|----------|
| ANTHROPIC_API_KEY | Yes | Claude (Haiku, Sonnet, Opus) |
| OPENAI_API_KEY | No | GPT-4o |
| GEMINI_API_KEY | No | Google Gemini |
| GROQ_API_KEY | No | Groq (fast inference) |
| CEREBRAS_API_KEY | No | Cerebras |
| MISTRAL_API_KEY | No | Mistral AI |
| OPENROUTER_API_KEY | No | OpenRouter (7 model variants) |
| XAI_API_KEY | No | xAI Grok |
| SAMBANOVA_API_KEY | No | SambaNova |
| TOGETHER_API_KEY | No | Together AI |
| HUGGINGFACE_TOKEN | No | Hugging Face |
| VOYAGE_API_KEY | No | Voyage AI (embeddings) |

## Data APIs (8 vars)
| Variable | Required | API |
|----------|----------|-----|
| ADZUNA_APP_ID + ADZUNA_API_KEY | No | Adzuna job data |
| JOOBLE_API_KEY | No | Jooble international jobs |
| FRED_API_KEY | No | Federal Reserve economic data |
| BLS_API_KEY | No | Bureau of Labor Statistics |
| ONET_API_KEY | No | O*NET occupational data |
| BEA_API_KEY | No | Bureau of Economic Analysis |
| USAJOBS_API_KEY + USAJOBS_EMAIL | No | USAJobs federal jobs |
| CENSUS_API_KEY | No | US Census Bureau |

## Web Scraping & Search (4 vars)
| Variable | Required | Service |
|----------|----------|---------|
| FIRECRAWL_API_KEY | No | Firecrawl web scraping |
| TAVILY_API_KEY | No | Tavily AI search |
| JINA_API_KEY | No | Jina AI reader |
| APIFY_API_TOKEN | No | Apify scraping |

## Infrastructure (10 vars)
| Variable | Required | Service |
|----------|----------|---------|
| PORT | Yes | Server port (default: 10000) |
| PYTHON_VERSION | Yes | Python version |
| LOG_LEVEL | No | Logging level |
| SUPABASE_URL + SUPABASE_KEY | No | Supabase database |
| UPSTASH_REDIS_URL + UPSTASH_REDIS_TOKEN | No | Upstash Redis cache |
| SENTRY_DSN | No | Sentry error tracking |
| SENTRY_AUTH_TOKEN | No | Sentry API access |
| POSTHOG_API_KEY | No | PostHog analytics |
| ELEVENLABS_API_KEY | No | ElevenLabs TTS |
| RESEND_API_KEY | No | Resend email |

## Not Yet Set (potential additions)
| Variable | Service | Status |
|----------|---------|--------|
| NVIDIA_NIM_API_KEY | NVIDIA NIM | Not set |
| SILICONFLOW_API_KEY | SiliconFlow | Not set |
| ZHIPU_API_KEY | Zhipu AI | Not set, Chinese provider |
| MOONSHOT_API_KEY | Moonshot | Not set, Chinese provider |
| QDRANT_API_KEY + QDRANT_URL | Qdrant vector DB | Not set |
| GOOGLE_SHEETS_CREDENTIALS | Google Sheets export | Not set |

Total: 48 active + 6 potential = 54 env vars
