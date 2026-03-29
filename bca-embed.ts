import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

const GEMINI_KEY = Deno.env.get('GEMINI_API_KEY');
const EMBED_URL  = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent';
const GEMINI_URL = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent';

const cors = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
};

// ── base64 → Uint8Array ───────────────────────────────────────────
function b64ToBytes(b64: string): Uint8Array {
  const bin = atob(b64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return bytes;
}

// ── Extração DOCX via XML interno ────────────────────────────────
async function extrairDOCX(bytes: Uint8Array): Promise<string> {
  let xmlContent = '';

  for (let i = 0; i < bytes.length - 30; i++) {
    if (bytes[i] === 0x50 && bytes[i+1] === 0x4B && bytes[i+2] === 0x03 && bytes[i+3] === 0x04) {
      const nameLen  = bytes[i+26] | (bytes[i+27] << 8);
      const extraLen = bytes[i+28] | (bytes[i+29] << 8);
      const name     = new TextDecoder('utf-8', { fatal: false }).decode(bytes.slice(i+30, i+30+nameLen));

      if (name === 'word/document.xml') {
        const compMethod = bytes[i+8]  | (bytes[i+9]  << 8);
        const compSize   = bytes[i+18] | (bytes[i+19] << 8) | (bytes[i+20] << 16) | (bytes[i+21] << 24);
        const uncompSize = bytes[i+22] | (bytes[i+23] << 8) | (bytes[i+24] << 16) | (bytes[i+25] << 24);
        const dataStart  = i + 30 + nameLen + extraLen;

        let xmlBytes: Uint8Array;
        if (compMethod === 0) {
          xmlBytes = bytes.slice(dataStart, dataStart + uncompSize);
        } else if (compMethod === 8) {
          try {
            const compressed = bytes.slice(dataStart, dataStart + compSize);
            const ds = new DecompressionStream('deflate-raw');
            const writer = ds.writable.getWriter();
            const reader = ds.readable.getReader();
            writer.write(compressed);
            writer.close();
            const parts: Uint8Array[] = [];
            while (true) {
              const { done, value } = await reader.read();
              if (done) break;
              parts.push(value);
            }
            const total = parts.reduce((n, c) => n + c.length, 0);
            xmlBytes = new Uint8Array(total);
            let off = 0;
            for (const p of parts) { xmlBytes.set(p, off); off += p.length; }
          } catch (_) { break; }
        } else { break; }

        xmlContent = new TextDecoder('utf-8', { fatal: false }).decode(xmlBytes);
        break;
      }
    }
  }

  if (!xmlContent) {
    const raw = new TextDecoder('utf-8', { fatal: false }).decode(bytes);
    const s = raw.indexOf('<w:body'), e = raw.indexOf('</w:body>');
    if (s >= 0 && e >= 0) xmlContent = raw.slice(s, e + 9);
    else return raw.replace(/[^\x20-\x7E\xA0-\xFF\n]/g, ' ').replace(/\s{4,}/g, ' ').trim();
  }

  return xmlContent
    .replace(/<w:br[^>]*\/>/g, '\n')
    .replace(/<w:p[ >][^]*?<\/w:p>/g, m => {
      const t = m.replace(/<w:t[^>]*>([^<]*)<\/w:t>/g, '$1').replace(/<[^>]+>/g, '');
      return t.trim() ? t.trim() + '\n' : '\n';
    })
    .replace(/<w:t[^>]*>([^<]*)<\/w:t>/g, '$1')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;/g, "'")
    .replace(/\n{3,}/g, '\n\n').replace(/[ \t]{3,}/g, ' ').trim();
}

// ── Extração de texto por tipo ────────────────────────────────────
async function extrairTexto(nome: string, base64?: string, textoPlano?: string): Promise<string> {
  if (textoPlano && textoPlano.trim().length > 30) return textoPlano.trim();
  if (!base64) return '';

  const bytes = b64ToBytes(base64);

  if (/\.pdf$/i.test(nome)) {
    const res = await fetch(`${GEMINI_URL}?key=${GEMINI_KEY}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        contents: [{ role: 'user', parts: [
          { inline_data: { mime_type: 'application/pdf', data: base64 } },
          { text: 'Extraia todo o texto deste documento mantendo a estrutura dos parágrafos e cláusulas. Retorne apenas o texto extraído, sem comentários.' }
        ]}],
        generationConfig: { temperature: 0, maxOutputTokens: 8192 }
      })
    });
    const data = await res.json();
    const t = data?.candidates?.[0]?.content?.parts?.[0]?.text?.trim() ?? '';
    return t.length > 30 ? t : '';
  }

  if (/\.(doc|docx)$/i.test(nome)) {
    const t = await extrairDOCX(bytes);
    return t.length > 30 ? t : '';
  }

  return new TextDecoder('utf-8', { fatal: false }).decode(bytes).trim();
}

// ── Chunking por parágrafos ───────────────────────────────────────
function chunkear(texto: string, maxWords = 600, overlap = 80): string[] {
  const paras = texto.split(/\n{2,}/).map(p => p.trim()).filter(p => p.length > 20);

  if (paras.length === 0) {
    const words = texto.split(/\s+/).filter(w => w.length > 0);
    const chunks: string[] = [];
    for (let i = 0; i < words.length; i += maxWords - overlap)
      chunks.push(words.slice(i, i + maxWords).join(' '));
    return chunks.filter(c => c.trim().length > 30);
  }

  const chunks: string[] = [];
  let current: string[] = [], wc = 0;

  for (const p of paras) {
    const pw = p.split(/\s+/).length;
    if (wc + pw > maxWords && current.length > 0) {
      chunks.push(current.join('\n\n'));
      const ov: string[] = []; let ow = 0;
      for (let i = current.length - 1; i >= 0; i--) {
        ow += current[i].split(/\s+/).length;
        ov.unshift(current[i]);
        if (ow >= overlap) break;
      }
      current = ov; wc = ow;
    }
    current.push(p); wc += pw;
  }
  if (current.length > 0) chunks.push(current.join('\n\n'));
  return chunks.filter(c => c.trim().length > 30);
}

// ── Embedding com retry ───────────────────────────────────────────
async function gerarEmbedding(texto: string): Promise<number[] | null> {
  for (let t = 0; t < 3; t++) {
    const res = await fetch(`${EMBED_URL}?key=${GEMINI_KEY}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'models/gemini-embedding-001',
        content: { parts: [{ text: texto.slice(0, 2000) }] },
        taskType: 'RETRIEVAL_DOCUMENT',
        outputDimensionality: 1536
      })
    });
    if (res.status === 429) { await new Promise(r => setTimeout(r, 6000 * (t + 1))); continue; }
    const data = await res.json();
    return data?.embedding?.values ?? null;
  }
  return null;
}

// ── Handler principal ─────────────────────────────────────────────
Deno.serve(async (req) => {
  if (req.method === 'OPTIONS') return new Response('ok', { headers: cors });

  function responder(body: object, status = 200) {
    return new Response(JSON.stringify(body), { status, headers: { ...cors, 'Content-Type': 'application/json' } });
  }

  try {
    if (!GEMINI_KEY) return responder({ error: 'GEMINI_API_KEY não configurada.' }, 500);

    const body = await req.json();
    const { nome_arquivo, texto, base64, tipo } = body;
    if (!nome_arquivo) return responder({ error: 'nome_arquivo é obrigatório.' }, 400);

    const supabase = createClient(
      Deno.env.get('SUPABASE_URL')!,
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!
    );

    // ── 1. Extrai texto (bucket feito depois para não impactar timeout) ──
    const textoExtraido = await extrairTexto(nome_arquivo, base64, texto);
    if (!textoExtraido || textoExtraido.length < 50) {
      return responder({
        error: 'Texto extraído vazio ou ilegível.',
        bucket: bucketStatus,
        debug: { nome_arquivo, tem_base64: !!base64, tem_texto: !!texto, len: textoExtraido?.length ?? 0 }
      }, 422);
    }

    // ── 3. Chunking ─────────────────────────────────────────────────
    const chunks = chunkear(textoExtraido);
    if (chunks.length === 0) return responder({ error: 'Nenhum chunk gerado.', bucket: bucketStatus }, 422);

    // ── 4. Upsert do registro na tabela documentos ──────────────────
    const { data: docData, error: docError } = await supabase
      .from('documentos')
      .upsert(
        { nome_arquivo, tipo: tipo ?? 'documento', storage_path: nome_arquivo },
        { onConflict: 'nome_arquivo', ignoreDuplicates: false }
      )
      .select('id')
      .single();

    if (docError || !docData) {
      return responder({ error: 'Erro ao salvar documento: ' + (docError?.message ?? 'sem id'), bucket: bucketStatus }, 500);
    }

    const documentoId = docData.id;

    // ── 5. Remove chunks antigos e insere novos em lotes paralelos ──
    await supabase.from('chunks').delete().eq('documento_id', documentoId);

    let salvos = 0;
    const erros: string[] = [];

    // Lotes de 4 embeddings em paralelo — reduz tempo total significativamente
    const BATCH = 4;
    for (let i = 0; i < chunks.length; i += BATCH) {
      const lote = chunks.slice(i, i + BATCH);
      const resultados = await Promise.all(
        lote.map((c, j) => gerarEmbedding(c).then(emb => ({ idx: i + j, conteudo: c, emb })))
      );
      for (const { idx, conteudo, emb } of resultados) {
        if (!emb) { erros.push(`chunk ${idx+1}: embedding falhou`); continue; }
        const { error: chunkError } = await supabase.from('chunks').insert({
          documento_id: documentoId,
          nome_arquivo,
          conteudo,
          embedding: emb,
          numero_chunk: idx
        });
        if (chunkError) erros.push(`chunk ${idx+1}: ${chunkError.message}`);
        else salvos++;
      }
    }

    if (salvos === 0) return responder({ error: 'Nenhum chunk salvo. Erros: ' + erros.join('; ') }, 500);

    // ── 6. Upload ao bucket (após indexação — não bloqueia em caso de erro) ──
    let bucketStatus = 'sem base64';
    if (base64) {
      try {
        const bytes = b64ToBytes(base64);
        const contentType = /\.pdf$/i.test(nome_arquivo) ? 'application/pdf'
          : /\.(doc|docx)$/i.test(nome_arquivo) ? 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
          : 'text/plain';
        const { error: storageError } = await supabase.storage
          .from('documentos-bca')
          .upload(nome_arquivo, bytes, { contentType, upsert: true });
        bucketStatus = storageError ? `erro: ${storageError.message}` : 'ok';
      } catch (e: any) {
        bucketStatus = `excecao: ${e.message}`;
      }
      console.log(`[bca-embed] bucket "${nome_arquivo}": ${bucketStatus}`);
    }

    return responder({
      response: `Indexado: ${salvos}/${chunks.length} chunks salvos.`,
      bucket: bucketStatus,
      documento_id: documentoId,
      chunks_salvos: salvos,
      chunks_total: chunks.length,
      erros: erros.length > 0 ? erros : undefined
    });

  } catch (err: any) {
    return responder({ error: err.message ?? 'Erro desconhecido' }, 500);
  }
});