export interface AlternateId {
  name: string;
  length: number;
  regex: RegExp;
  /** Convert alternate ID to the primary ID for lookup */
  toPrimaryId: (id: string) => string;
}

export interface SourceConfig {
  country: string;
  institution: string;
  dataset: string;
  source: string;
  /** R2 base name for .idx / .bin / search files */
  baseName: string;
  /** API route path: /{country}/{institution}/{dataset} */
  routePath: string;
  primaryId: {
    name: string;
    length: number;
    regex: RegExp;
    prefixLength: number;
    /** Normalize user input before validation+lookup (e.g. zero-pad) */
    normalize?: (id: string) => string;
  };
  alternateIds: AlternateId[];
  fieldNames: readonly string[];
  /** Index in fieldNames used for search display */
  searchFieldIndex: number;
}

export function dniToRuc(dni: string): string {
  const weights = [5, 4, 3, 2, 7, 6, 5, 4, 3, 2];
  const digits = ('10' + dni).split('').map(Number);
  let sum = 0;
  for (let i = 0; i < 10; i++) sum += digits[i] * weights[i];
  const remainder = 11 - (sum % 11);
  const check = remainder === 10 ? 0 : remainder === 11 ? 1 : remainder;
  return '10' + dni + check;
}

/**
 * Source registry — auto-generated from sources/*.yaml
 * DO NOT EDIT MANUALLY — run: npx tsx src/imports/generate-sources.ts
 */
export const sources: SourceConfig[] = [
  {
    country: 'co', institution: 'rues', dataset: 'registry',
    source: 'rues', baseName: 'co-rues',
    routePath: '/co/rues/registry',
    primaryId: { name: 'nit', length: 10, regex: /^\d{10}$/, prefixLength: 5, normalize: (id: string) => id.padStart(10, '0') },
    alternateIds: [],
    fieldNames: ['razon_social', 'estado_matricula', 'organizacion_juridica', 'cod_ciiu_act_econ_pri', 'fecha_matricula', 'camara_comercio', 'representante_legal', 'num_identificacion_representante_legal', 'sigla', 'tipo_sociedad', 'fecha_renovacion'],
    searchFieldIndex: 0,
  },
  {
    country: 'pe', institution: 'oece', dataset: 'tenders',
    source: 'oece-tenders', baseName: 'pe-oece-tenders',
    routePath: '/pe/oece/tenders',
    primaryId: { name: 'ocid', length: 30, regex: /.*/, prefixLength: 5 },
    alternateIds: [],
    fieldNames: ['description', 'status', 'category', 'amount', 'buyer', 'method', 'date'],
    searchFieldIndex: 0,
  },
  {
    country: 'pe', institution: 'osce', dataset: 'fines',
    source: 'osce-fines', baseName: 'pe-osce-fines',
    routePath: '/pe/osce/fines',
    primaryId: { name: 'ruc', length: 11, regex: /^\d{11}$/, prefixLength: 5 },
    alternateIds: [],
    fieldNames: ['name', 'date_start', 'date_end', 'resolution', 'infraction_code', 'detail', 'amount'],
    searchFieldIndex: 0,
  },
  {
    country: 'pe', institution: 'osce', dataset: 'sanctioned',
    source: 'osce-sanctioned', baseName: 'pe-osce-sanctioned',
    routePath: '/pe/osce/sanctioned',
    primaryId: { name: 'ruc', length: 11, regex: /^\d{11}$/, prefixLength: 5 },
    alternateIds: [],
    fieldNames: ['name', 'date_start', 'date_end', 'resolution', 'infraction_code', 'detail'],
    searchFieldIndex: 0,
  },
  {
    country: 'pe', institution: 'redam', dataset: 'registry',
    source: 'redam-registry', baseName: 'pe-redam-registry',
    routePath: '/pe/redam/registry',
    primaryId: { name: 'dni', length: 8, regex: /^\d{8}$/, prefixLength: 4 },
    alternateIds: [],
    fieldNames: ['ape_paterno', 'ape_materno', 'nombres', 'tipo_doc', 'fecha_registro', 'full_name'],
    searchFieldIndex: 5,
  },
  {
    country: 'pe', institution: 'servir', dataset: 'sanctions',
    source: 'undefined', baseName: 'pe-servir-sanctions',
    routePath: '/pe/servir/sanctions',
    primaryId: { name: 'dni', length: 8, regex: /^\d{8}$/, prefixLength: 4 },
    alternateIds: [],
    fieldNames: ['nombre', 'estado_sancion', 'tipo_sancion', 'entidad', 'fecha_inicio', 'fecha_fin', 'sanctions_count'],
    searchFieldIndex: 0,
  },
  {
    country: 'pe', institution: 'sunat', dataset: 'coactiva',
    source: 'sunat-coactiva', baseName: 'pe-sunat-coactiva',
    routePath: '/pe/sunat/coactiva',
    primaryId: { name: 'ruc', length: 11, regex: /^\d{11}$/, prefixLength: 5 },
    alternateIds: [],
    fieldNames: ['name', 'commercial_name', 'dependency', 'amount', 'legal_reps'],
    searchFieldIndex: 0,
  },
  {
    country: 'pe', institution: 'sunat', dataset: 'padron',
    source: 'sunat-padron', baseName: 'pe-sunat-padron',
    routePath: '/pe/sunat/padron',
    primaryId: { name: 'ruc', length: 11, regex: /^\d{11}$/, prefixLength: 5 },
    alternateIds: [
      { name: 'dni', length: 8, regex: /^\d{8}$/, toPrimaryId: dniToRuc },
    ],
    fieldNames: ['razon_social', 'estado', 'condicion', 'ubigeo', 'tipo_via', 'nombre_via', 'codigo_zona', 'tipo_zona', 'numero', 'interior', 'lote', 'departamento', 'manzana', 'kilometro'],
    searchFieldIndex: 0,
  },
];

/** Find a source by its route path components */
export function findSource(country: string, institution: string, dataset: string): SourceConfig | undefined {
  return sources.find(s => s.country === country && s.institution === institution && s.dataset === dataset);
}

/** Find all sources for a country */
export function findSourcesByCountry(country: string): SourceConfig[] {
  return sources.filter(s => s.country === country);
}
