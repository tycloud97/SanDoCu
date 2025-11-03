import { Tab } from "@headlessui/react";
import { Fragment, useEffect, useMemo, useState } from "react";
import { MagnifyingGlassIcon, EyeIcon, EyeSlashIcon } from "@heroicons/react/24/outline";
import Papa from "papaparse";

type GroupKey = "chotot" | "fb_group" | "fb_market";

type Item = {
  id: string;
  title: string;
  description: string;
  price: number | null;
  location: string;
  seller?: string;
  url?: string;
  image?: string;
  crawl_time?: string;
  sourceKey: GroupKey;
  sourceLabel: string;
};

type TabKey = "all" | "unviewed" | "viewed";

const SOURCES: { key: GroupKey; label: string; url: string }[] = [
  { key: "chotot", label: "Chợ Tốt", url: "/data/sources/chotot.csv" },
  { key: "fb_group", label: "Facebook Group", url: "/data/sources/facebook_group.csv" },
  { key: "fb_market", label: "Facebook Market", url: "/data/sources/facebook_marketplace.csv" },
];
const SUGGESTED_TAGS = ["sony", "dji", "canon", "nikon"];

function classNames(...classes: (string | false | null | undefined)[]) {
  return classes.filter(Boolean).join(" ");
}

function useMultiCSV(sources: { key: GroupKey; label: string; url: string }[]) {
  const [items, setItems] = useState<Item[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    const srcKey = JSON.stringify(sources.map((s) => [s.key, s.url]));

    const parseOne = (src: { key: GroupKey; label: string; url: string }) =>
      new Promise<Item[]>((resolve, reject) => {
        Papa.parse<any>(src.url, {
          download: true,
          header: true,
          skipEmptyLines: true,
          transformHeader: (h) => h.replace(/\uFEFF/g, "").trim().toLowerCase(),
          complete: (res) => {
            const rows = (res.data as any[]) || [];
            const parsed = rows.map((row) => normalizeItem(row, src.key, src.label));
            resolve(parsed);
          },
          error: (err) => reject(err),
        });
      });

    Promise.allSettled(sources.map((s) => parseOne(s))).then((results) => {
      if (cancelled) return;
      const all: Item[] = [];
      const errs: string[] = [];
      for (const r of results) {
        if (r.status === "fulfilled") all.push(...r.value);
        else errs.push(r.reason?.message || String(r.reason));
      }
      setItems(all);
      setError(errs.length ? errs.join("; ") : null);
      setLoading(false);
    });

    return () => {
      void srcKey; // keep linter calm
      cancelled = true;
    };
  }, [sources]);

  return { items, loading, error };
}

function cleanStr(v: any): string {
  return String(v ?? "").replace(/\uFEFF/g, "").trim();
}

function optStr(v: any): string | undefined {
  const s = cleanStr(v);
  if (!s) return undefined;
  const lower = s.toLowerCase();
  if (lower === "null" || lower === "undefined" || lower === "none") return undefined;
  return s;
}

function parsePrice(v: any): number | null {
  const raw = cleanStr(v);
  if (!raw) return null;
  // Remove all non-digits to handle "₫1,234", "1.234.000", etc. (VND doesn't use decimals commonly)
  const digits = raw.replace(/[^0-9]/g, "");
  if (!digits) return null;
  const n = Number(digits);
  return Number.isFinite(n) ? n : null;
}

function normalizeItem(row: any, sourceKey: GroupKey, sourceLabel: string): Item {
  const rawId = cleanStr(row.id);
  const title = cleanStr(row.title);
  const description = cleanStr(row.description);
  const location = cleanStr(row.location);
  const seller = optStr(row.seller);
  const url = optStr(row.post_url);
  const image = optStr(row.image);
  const price = parsePrice(row.price);
  const crawl_time = optStr(row.crawl_time);
  // Ensure globally unique id across sources
  const id = rawId
  return {
    id,
    title,
    description,
    price: Number.isFinite(price) ? price : null,
    location,
    seller,
    url,
    image,
    crawl_time,
    sourceKey,
    sourceLabel,
  };
}

function slug(s: string) {
  const base = s
    .toLowerCase()
    .normalize("NFD")
    // Remove combining diacritics (broad browser support)
    .replace(/[\u0300-\u036f]/g, "");
  const mapped = base.replace(/[đð]/g, "d");
  return mapped
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)+/g, "");
}

function useViewed(ids: string[]) {
  const KEY = "san-do-cu:viewed";
  const [viewed, setViewed] = useState<Record<string, boolean>>({});
  const idsKey = useMemo(() => {
    const uniq = Array.from(new Set(ids.filter(Boolean)));
    uniq.sort();
    return uniq.join("|");
  }, [ids]);

  useEffect(() => {
    try {
      const raw = localStorage.getItem(KEY);
      setViewed(raw ? JSON.parse(raw) : {});
    } catch {}
  }, []);

  const mark = (id: string, v: boolean) =>
    setViewed((prev) => {
      if (!id) return prev;
      const next = { ...prev, [id]: v };
      try {
        localStorage.setItem(KEY, JSON.stringify(next));
      } catch {}
      return next;
    });

  const markMany = (ids: string[], v: boolean) =>
    setViewed((prev) => {
      const next = { ...prev };
      for (const id of ids) if (id) next[id] = v;
      try {
        localStorage.setItem(KEY, JSON.stringify(next));
      } catch {}
      return next;
    });

  const isViewed = (id: string) => !!viewed[id];

  const clear = () => {
    setViewed(() => {
      try {
        localStorage.setItem(KEY, JSON.stringify({}));
      } catch {}
      return {};
    });
  };

  // Ensure keys exist for current ids without extra writes/renders
  useEffect(() => {
    setViewed((prev) => {
      let changed = false;
      const next = { ...prev };
      for (const id of ids) {
        if (id && !(id in next)) {
          next[id] = !!prev[id];
          changed = true;
        }
      }
      return changed ? next : prev;
    });
  }, [idsKey]);

  return { isViewed, mark, markMany, clear, viewed };
}

export default function App() {
  const { items, loading, error } = useMultiCSV(SOURCES);
  const [query, setQuery] = useState("");
  const [activeTab, setActiveTab] = useState<TabKey>("all");
  const [selectedTags, setSelectedTags] = useState<string[]>([]);
  const [selectedGroups, setSelectedGroups] = useState<GroupKey[]>(SOURCES.map((s) => s.key));
  const { isViewed, mark, markMany, clear } = useViewed(items.map((i) => i.id));

  const terms = useMemo(() => {
    const inputTokens = query
      .toLowerCase()
      .split(/[\s,]+/)
      .map((t) => t.trim())
      .filter(Boolean);
    const all = [...selectedTags, ...inputTokens];
    // unique
    return Array.from(new Set(all));
  }, [query, selectedTags]);

  // Đếm số lượng theo từng nguồn với các điều kiện lọc khác (query/tabs), bỏ qua lọc theo nguồn
  const groupCounts = useMemo(() => {
    const counts: Record<GroupKey, number> = { chotot: 0, fb_group: 0, fb_market: 0 };
    const matches = (it: Item) => {
      if (terms.length === 0) return true;
      const hay = `${it.title} ${it.description}`.toLowerCase();
      return terms.some((k) => hay.includes(k));
    };
    for (const it of items) {
      if (!matches(it)) continue;
      if (activeTab === "viewed" && !isViewed(it.id)) continue;
      if (activeTab === "unviewed" && isViewed(it.id)) continue;
      counts[it.sourceKey]++;
    }
    return counts;
  }, [items, terms, activeTab, isViewed]);

  const filtered = useMemo(() => {
    // If no group is selected, show nothing (explicit behavior)
    if (selectedGroups.length === 0) return [] as Item[];
    const matches = (it: Item) => {
      if (terms.length === 0) return true; // no filter
      const hay = `${it.title} ${it.description}`.toLowerCase();
      // OR logic: at least one term matches
      return terms.some((k) => hay.includes(k));
    };

    return items.filter((it) => {
      if (selectedGroups.length && !selectedGroups.includes(it.sourceKey)) return false;
      if (!matches(it)) return false;
      if (activeTab === "viewed") return isViewed(it.id);
      if (activeTab === "unviewed") return !isViewed(it.id);
      return true;
    });
  }, [items, terms, activeTab, isViewed, selectedGroups]);

  return (
    <div className="min-h-full">
      <Header onClearViewed={clear} />
      <main className="container-safe py-3 sm:py-4">
        <SearchBar query={query} setQuery={setQuery} total={items.length} />
        <div className="mt-2">
          <GroupFilter
            sources={SOURCES}
            selected={selectedGroups}
            counts={groupCounts}
            onToggle={(g) =>
              setSelectedGroups((prev) => (prev.includes(g) ? prev.filter((x) => x !== g) : [...prev, g]))
            }
          />
        </div>
        <TagFilter
          suggested={SUGGESTED_TAGS}
          selected={selectedTags}
          onToggle={(tag) =>
            setSelectedTags((prev) =>
              prev.includes(tag) ? prev.filter((t) => t !== tag) : [...prev, tag]
            )
          }
        />

        <div className="mt-3 sm:mt-4">
          <Tab.Group onChange={(i) => setActiveTab(["all", "unviewed", "viewed"][i] as TabKey)}>
            <Tab.List className="flex space-x-2 rounded-xl bg-white p-1 shadow">
              {[
                { key: "all", label: `Tất cả (${items.length})` },
                { key: "unviewed", label: `Chưa xem (${items.filter((i) => !isViewed(i.id)).length})` },
                { key: "viewed", label: `Đã xem (${items.filter((i) => isViewed(i.id)).length})` },
              ].map((t, idx) => (
                <Tab
                  key={t.key}
                  className={({ selected }) =>
                    classNames(
                      "w-full rounded-lg py-2.5 text-sm font-medium leading-5",
                      selected
                        ? "bg-indigo-600 text-white shadow"
                        : "text-indigo-700 hover:bg-indigo-100"
                    )
                  }
                >
                  {t.label}
                </Tab>
              ))}
            </Tab.List>
            <BulkActions
              count={filtered.length}
              onMarkAll={() => markMany(filtered.map((i) => i.id), true)}
              onUnmarkAll={() => markMany(filtered.map((i) => i.id), false)}
            />
            <Tab.Panels className="mt-3">
              <Tab.Panel>
                <ItemList items={filtered} isViewed={isViewed} onToggle={mark} terms={terms} />
              </Tab.Panel>
              <Tab.Panel>
                <ItemList items={filtered} isViewed={isViewed} onToggle={mark} terms={terms} />
              </Tab.Panel>
              <Tab.Panel>
                <ItemList items={filtered} isViewed={isViewed} onToggle={mark} terms={terms} />
              </Tab.Panel>
            </Tab.Panels>
          </Tab.Group>
        </div>

        {loading && (
          <p className="text-sm text-gray-500 mt-4">Đang tải dữ liệu…</p>
        )}
        {error && (
          <p className="text-sm text-red-600 mt-2">Lỗi: {error}</p>
        )}
      </main>
    </div>
  );
}

function Header({ onClearViewed }: { onClearViewed: () => void }) {
  return (
    <header className="sticky top-0 z-10 bg-white/90 backdrop-blur supports-[backdrop-filter]:bg-white/60 border-b">
      <div className="container-safe py-3 flex items-center justify-between">
        <h1 className="text-lg font-semibold text-gray-900">Săn Đồ Cũ</h1>
        <button
          onClick={onClearViewed}
          className="text-xs sm:text-sm rounded-md border px-2.5 py-1.5 text-gray-700 hover:bg-gray-50"
        >
          Xóa dấu đã xem
        </button>
      </div>
    </header>
  );
}

function SearchBar({
  query,
  setQuery,
  total,
}: {
  query: string;
  setQuery: (s: string) => void;
  total: number;
}) {
  return (
    <div className="bg-white rounded-lg border shadow-sm p-2 sm:p-3">
      <label className="flex items-center gap-2">
        <MagnifyingGlassIcon className="h-5 w-5 text-gray-500" />
        <input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          className="w-full bg-transparent outline-none placeholder:text-gray-400 text-sm"
          placeholder="Tìm theo tiêu đề, mô tả… (có thể nhập từ khóa, cách nhau bằng dấu phẩy)"
          aria-label="Tìm kiếm"
        />
      </label>
      <p className="mt-1 text-xs text-gray-500">Tổng số: {total}</p>
    </div>
  );
}

function ItemList({
  items,
  isViewed,
  onToggle,
  terms,
}: {
  items: Item[];
  isViewed: (id: string) => boolean;
  onToggle: (id: string, v: boolean) => void;
  terms: string[];
}) {
  if (items.length === 0) {
    return (
      <div className="text-center text-sm text-gray-500 bg-white border rounded-lg p-6">
        Không có kết quả phù hợp.
      </div>
    );
  }

  return (
    <ul className="space-y-2">
      {items.map((it) => (
        <li key={it.id || `${it.title}-${it.location}`} className="bg-white border rounded-lg p-3 shadow-sm">
          <div className="flex items-start gap-3">
            <button
              onClick={() => onToggle(it.id, !isViewed(it.id))}
              className={classNames(
                "shrink-0 inline-flex items-center justify-center h-9 w-9 rounded-md border",
                isViewed(it.id)
                  ? "bg-green-50 border-green-200 text-green-700"
                  : "bg-gray-50 border-gray-200 text-gray-600"
              )}
              title={isViewed(it.id) ? "Bỏ đánh dấu đã xem" : "Đánh dấu đã xem"}
            >
              {isViewed(it.id) ? (
                <EyeIcon className="h-5 w-5" />
              ) : (
                <EyeSlashIcon className="h-5 w-5" />
              )}
            </button>

            <div className="min-w-0 flex-1">
              <div className="flex items-baseline justify-between gap-2">
                <h3 className="text-sm font-semibold text-gray-900 truncate">
                  <Highlighted text={it.title || "(Không có tiêu đề)"} terms={terms} />
                </h3>
                {it.price != null && (
                  <span className="text-sm font-medium text-indigo-700">{formatCurrency(it.price)}</span>
                )}
              </div>
              <div className="mt-1 flex items-center gap-2 flex-wrap">
                <span className={classNames("inline-flex items-center px-2 py-0.5 rounded border text-[11px]", groupStyles(it.sourceKey))}>
                  {it.sourceLabel}
                </span>
                {it.crawl_time && (
                  <span className="text-[11px] text-gray-500">{it.crawl_time}</span>
                )}
              </div>
              <div className="flex items-start gap-3 mt-1">
                {it.image ? (
                  <img
                    src={it.image}
                    alt=""
                    className="h-16 w-16 rounded object-cover border"
                    loading="lazy"
                    referrerPolicy="no-referrer"
                  />
                ) : null}
                <div className="min-w-0 flex-1">
                  {it.description && (
                    <p className="mt-1 text-sm text-gray-700 line-clamp-3">
                      <Highlighted text={it.description} terms={terms} />
                    </p>
                  )}
                  <div className="mt-2 text-xs text-gray-600 flex flex-wrap items-center gap-2">
                    {it.location && <span>{it.location}</span>}
                    {it.seller && (
                      <span className="text-gray-400">•</span>
                    )}
                    {it.seller && <span>Người bán: {it.seller}</span>}
                    {it.url && (
                      <>
                        <span className="text-gray-400">•</span>
                        <a
                          href={it.url}
                          target="_blank"
                          rel="noreferrer"
                          className="text-indigo-600 hover:underline"
                        >
                          Xem bài
                        </a>
                      </>
                    )}
                  </div>
                </div>
              </div>
            </div>
          </div>
        </li>
      ))}
    </ul>
  );
}

function formatCurrency(v: number) {
  try {
    return new Intl.NumberFormat("vi-VN", { style: "currency", currency: "VND", maximumFractionDigits: 0 }).format(v);
  } catch {
    return `${v}`;
  }
}

function TagFilter({
  suggested,
  selected,
  onToggle,
}: {
  suggested: string[];
  selected: string[];
  onToggle: (tag: string) => void;
}) {
  if (!suggested?.length) return null;
  return (
    <div className="mt-2 flex flex-wrap gap-2">
      {suggested.map((tag) => {
        const active = selected.includes(tag);
        return (
          <button
            key={tag}
            onClick={() => onToggle(tag)}
            className={classNames(
              "text-xs px-2.5 py-1.5 rounded-full border transition",
              active
                ? "bg-indigo-600 text-white border-indigo-600"
                : "bg-white text-indigo-700 border-indigo-200 hover:bg-indigo-50"
            )}
            aria-pressed={active}
          >
            #{tag}
          </button>
        );
      })}
    </div>
  );
}

function BulkActions({
  count,
  onMarkAll,
  onUnmarkAll,
}: {
  count: number;
  onMarkAll: () => void;
  onUnmarkAll: () => void;
}) {
  return (
    <div className="mt-3 flex items-center justify-between">
      <div className="text-xs text-gray-600">Đang hiển thị: {count} mục</div>
      <div className="flex gap-2">
        <button
          onClick={onMarkAll}
          className="text-xs rounded-md border px-2.5 py-1.5 text-gray-700 hover:bg-gray-50"
        >
          Đánh dấu tất cả đã xem
        </button>
        <button
          onClick={onUnmarkAll}
          className="text-xs rounded-md border px-2.5 py-1.5 text-gray-700 hover:bg-gray-50"
        >
          Bỏ đánh dấu tất cả
        </button>
      </div>
    </div>
  );
}

function Highlighted({ text, terms }: { text: string; terms: string[] }) {
  const parts = useMemo(() => highlightSplit(text, terms), [text, terms.join(",")]);
  return (
    <Fragment>
      {parts.map((p, i) =>
        p.match ? (
          <mark key={i} className="bg-yellow-200 rounded px-0.5">
            {p.text}
          </mark>
        ) : (
          <Fragment key={i}>{p.text}</Fragment>
        )
      )}
    </Fragment>
  );
}

function highlightSplit(text: string, terms: string[]) {
  if (!terms?.length) return [{ text, match: false }];
  const tokens = terms
    .map((t) => t.trim().toLowerCase())
    .filter(Boolean)
    .map(escapeRegExp);
  if (tokens.length === 0) return [{ text, match: false }];
  const re = new RegExp(`(${tokens.join("|")})`, "gi");
  const out: { text: string; match: boolean }[] = [];
  let lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) {
    const start = m.index;
    const end = start + m[0].length;
    if (start > lastIndex) out.push({ text: text.slice(lastIndex, start), match: false });
    out.push({ text: text.slice(start, end), match: true });
    lastIndex = end;
  }
  if (lastIndex < text.length) out.push({ text: text.slice(lastIndex), match: false });
  return out;
}

function escapeRegExp(s: string) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function groupStyles(key: GroupKey) {
  switch (key) {
    case "chotot":
      return "bg-orange-50 text-orange-700 border-orange-200";
    case "fb_group":
      return "bg-blue-50 text-blue-700 border-blue-200";
    case "fb_market":
      return "bg-purple-50 text-purple-700 border-purple-200";
    default:
      return "bg-gray-50 text-gray-700 border-gray-200";
  }
}

function GroupFilter({
  sources,
  selected,
  counts,
  onToggle,
}: {
  sources: { key: GroupKey; label: string; url: string }[];
  selected: GroupKey[];
  counts: Record<GroupKey, number>;
  onToggle: (key: GroupKey) => void;
}) {
  return (
    <div className="bg-white rounded-lg border shadow-sm p-2">
      <div className="text-xs font-medium text-gray-700 mb-1">Nguồn dữ liệu</div>
      <fieldset className="flex flex-wrap gap-4">
        {sources.map((s) => {
          const checked = selected.includes(s.key);
          return (
            <label key={s.key} className="inline-flex items-center gap-2 text-sm text-gray-700">
              <input
                type="checkbox"
                className="h-4 w-4 rounded border-gray-300 text-indigo-600 focus:ring-indigo-500"
                checked={checked}
                onChange={() => onToggle(s.key)}
              />
              <span>
                {s.label} <span className="text-gray-400">({counts[s.key] ?? 0})</span>
              </span>
            </label>
          );
        })}
      </fieldset>
    </div>
  );
}
