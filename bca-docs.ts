import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

const GEMINI_KEY = Deno.env.get('GEMINI_API_KEY');
const EMBED_URL  = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent';
const GEMINI_URL = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent';

const cors = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
};

// ── Embedding da query com retry em 429 ───────────────────────────
async function gerarEmbedding(texto: string): Promise<number[] | null> {
  for (let t = 0; t < 3; t++) {
    const res = await fetch(`${EMBED_URL}?key=${GEMINI_KEY}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model:    'models/gemini-embedding-001',
        content:  { parts: [{ text: texto }] },
        taskType: 'RETRIEVAL_QUERY',
        outputDimensionality: 1536
      })
    });
    if (res.status === 429) {
      await new Promise(r => setTimeout(r, 5000 * (t + 1)));
      continue;
    }
    const data = await res.json();
    return data?.embedding?.values ?? null;
  }
  return null;
}

// ── Chamada Gemini com retry em 429 ───────────────────────────────
async function chamarGemini(
  systemPrompt: string,
  userContent: string
): Promise<{ texto: string | null; status: number; erro: any }> {
  const DELAYS = [10000, 25000, 45000];

  for (let t = 0; t < 3; t++) {
    const res = await fetch(`${GEMINI_URL}?key=${GEMINI_KEY}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        system_instruction: { parts: [{ text: systemPrompt }] },
        contents: [{ role: 'user', parts: [{ text: userContent }] }],
        generationConfig: {
          temperature:     0.1,
          maxOutputTokens: 8192
        }
      })
    });

    const data = await res.json();

    if (res.status === 429) {
      if (t < 2) { await new Promise(r => setTimeout(r, DELAYS[t])); continue; }
      return { texto: null, status: 429, erro: data?.error };
    }

    const texto = data?.candidates?.[0]?.content?.parts?.[0]?.text ?? null;
    return { texto, status: res.status, erro: data?.error ?? null };
  }

  return { texto: null, status: 429, erro: 'Limite de requisições Gemini atingido.' };
}

// ══════════════════════════════════════════════════════════════════
Deno.serve(async (req) => {
  if (req.method === 'OPTIONS') return new Response('ok', { headers: cors });

  function responder(body: object, status = 200) {
    return new Response(JSON.stringify(body), {
      status,
      headers: { ...cors, 'Content-Type': 'application/json' }
    });
  }

  try {
    if (!GEMINI_KEY) {
      return responder({ error: 'GEMINI_API_KEY não configurada nos secrets do Supabase.' }, 500);
    }

    const body = await req.json();
    const { message, mode } = body;

    const supabase = createClient(
      Deno.env.get('SUPABASE_URL')!,
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!
    );

    // ── Modo: listar documentos indexados ─────────────────────────
    if (mode === 'list-docs') {
      const { data: arquivos, error: storageError } = await supabase
        .storage.from('documentos-bca')
        .list('', { limit: 200, sortBy: { column: 'name', order: 'asc' } });

      if (storageError) {
        return responder({ error: 'Erro ao listar bucket: ' + storageError.message }, 500);
      }

      const arquivosFiltrados = (arquivos ?? []).filter(
        f => f.name && /\.(pdf|doc|docx|txt)$/i.test(f.name)
      );

      const { data: docsIndexados } = await supabase
        .from('documentos')
        .select('nome_arquivo, id, criado_em');

      const indexadosMap = new Map<string, any>();
      if (docsIndexados) {
        for (const d of docsIndexados) indexadosMap.set(d.nome_arquivo, d);
      }

      const { data: chunkCounts } = await supabase.from('chunks').select('documento_id');
      const chunksPorDoc = new Map<string, number>();
      if (chunkCounts) {
        for (const c of chunkCounts) {
          chunksPorDoc.set(c.documento_id, (chunksPorDoc.get(c.documento_id) ?? 0) + 1);
        }
      }

      const resultado = arquivosFiltrados.map(f => {
        const doc    = indexadosMap.get(f.name);
        const chunks = doc ? (chunksPorDoc.get(doc.id) ?? 0) : 0;
        return {
          nome:        f.name,
          tamanho_kb:  f.metadata?.size ? Math.round(f.metadata.size / 1024) : null,
          indexado:    !!doc && chunks > 0,
          chunks,
          indexado_em: doc?.criado_em ?? null,
        };
      });

      return responder({
        response:  'ok',
        arquivos:  resultado,
        total:     resultado.length,
        indexados: resultado.filter(f => f.indexado).length,
      });
    }

    // ── Modos chat e comparador ───────────────────────────────────
    if (!message) throw new Error('message é obrigatório.');

    // 1. Embedding da query
    const queryEmbedding = await gerarEmbedding(message);

    // 2. Busca RAG — ajusta match_count e limite de contexto por modo
    let contextText = '';
    let chunksDebug: object = {};
    const matchCount  = mode === 'comparador' ? 8 : 6;
    const limiteChars = mode === 'comparador' ? 12000 : 8000;

    if (queryEmbedding) {
      const { data: chunks, error: rpcError } = await supabase.rpc('buscar_chunks_similares', {
        query_embedding: queryEmbedding,
        match_count:     matchCount,
        match_threshold: 0.3
      });

      chunksDebug = {
        rpc_error:    rpcError?.message ?? null,
        chunks_found: chunks?.length ?? 0,
        arquivos:     chunks?.map((c: any) => c.nome_arquivo) ?? [],
      };

      if (chunks && chunks.length > 0) {
        const raw = chunks
          .map((c: any, i: number) => `[Trecho ${i + 1} — ${c.nome_arquivo}]\n${c.conteudo}`)
          .join('\n\n---\n\n');
        contextText = raw.slice(0, limiteChars);
      }
    }

    // 3. System prompts por modo
    const systemPromptChat = `Você é o assistente jurídico interno do escritório BCA (Barbur Carneiro Advogados).
Sua função é responder consultas sobre cláusulas contratuais com base nos documentos indexados na base BCA.
Responda sempre em português formal e objetivo.

REGRAS OBRIGATÓRIAS:
- Priorize sempre o conteúdo da base de conhecimento BCA.
- Para cada cláusula mencionada ou solicitada, indique obrigatoriamente:
  1. O texto completo ou substancial da cláusula conforme consta na base BCA
  2. O nome exato do arquivo/contrato de origem (ex: "Contrato de Prestação de Serviços BCA 2024.pdf")
  3. Cláusulas relacionadas ao mesmo tema que constem em outros documentos da base
  4. Se a cláusula é elemento padrão (presente na maioria dos modelos BCA) ou específica (presente em contratos pontuais)
- Se não localizar a cláusula na base, informe explicitamente e responda com conhecimento jurídico geral, sinalizando que a resposta não tem origem nos documentos BCA.
- Não emita pareceres ou recomendações jurídicas além do que consta na base.
- Não invente conteúdo de cláusulas. Se o texto não estiver na base, diga que não foi localizado.

ESTRUTURA DA RESPOSTA:

📄 CLÁUSULA NA BASE BCA
[Texto completo ou substancial da cláusula, conforme indexado]

📁 ORIGEM
[Nome exato do arquivo — ex: "Tag Along — Contrato Societário Modelo BCA.pdf"]

🔗 CLÁUSULAS RELACIONADAS
[Outras cláusulas sobre o mesmo tema em outros documentos da base, com nome do arquivo de cada uma]

📌 STATUS NA BASE BCA
[Padrão — presente na maioria dos modelos] ou [Específica — presente em contratos pontuais]`;

    const systemPromptComparador = `Você é um especialista em análise comparativa de contratos do escritório BCA (Barbur Carneiro Advogados).
Receberá dois blocos de texto claramente separados:
— MODELOS BCA: trechos dos documentos padrão do escritório
— DOCUMENTO ANALISADO: texto enviado pelo usuário para comparação

Sua função exclusiva é comparar os dois blocos e apresentar as divergências de forma objetiva e visualmente limpa.
Responda sempre em português formal.

REGRAS OBRIGATÓRIAS:
- Não use os termos "risco", "problemático" ou "perigoso".
- Não repita as cláusulas na íntegra. Extraia apenas o trecho específico onde há divergência.
- Destaque o trecho divergente com **negrito e sublinhado**: use a sintaxe markdown **__trecho divergente__**
- Se uma cláusula do DOCUMENTO ANALISADO não tiver equivalente nos MODELOS BCA, classifique como AUSENTE NA BASE.
- Não emita pareceres jurídicos além da comparação objetiva.
- Não inclua seção de "Modelo de Referência".

ESTRUTURA OBRIGATÓRIA DA RESPOSTA — siga exatamente esta ordem:

🔍 CLÁUSULAS CORRESPONDENTES NA BASE BCA
Liste cada cláusula dos MODELOS BCA que se relaciona com o DOCUMENTO ANALISADO.
Formato: nome da cláusula — arquivo de origem

⚖️ ANÁLISE COMPARATIVA
Para cada cláusula do DOCUMENTO ANALISADO, use um dos marcadores:
✅ CONFORME — redação equivalente ao modelo BCA
✏️ DIVERGENTE — descreva a diferença em uma linha objetiva (ex: "prazo alterado de 30 para 90 dias")
🆕 AUSENTE NA BASE — sem equivalente nos modelos BCA

📋 QUADRO DE DIVERGÊNCIAS
Apresente uma tabela markdown com TRÊS colunas:

| Cláusula | Modelo BCA | Documento Analisado |
|----------|-----------|-----------------|

Regras da tabela:
- Coluna "Cláusula": nome da cláusula (ex: "Confidencialidade", "Prazo", "Multa")
- Coluna "Modelo BCA": transcreva APENAS o trecho específico que difere, com **__destaque__** na parte divergente. Se CONFORME, escreva "—"
- Coluna "Documento Analisado": transcreva APENAS o trecho específico que difere, com **__destaque__** na parte divergente. Se AUSENTE NA BASE, escreva "Não consta"
- Não repita a cláusula inteira. Use reticências (...) para indicar que o trecho foi extraído de um contexto maior
- Exemplo de linha: | Prazo de vigência | "...prazo de **__30 dias__**..." | "...prazo de **__90 dias__**..." |`;

    const systemPrompt = mode === 'comparador' ? systemPromptComparador : systemPromptChat;

    // 4. Monta conteúdo do usuário
    const semContexto = mode === 'comparador'
      ? 'ATENÇÃO: Nenhum trecho correspondente localizado na base BCA. Informe isso ao usuário e analise apenas com base no conhecimento jurídico geral, indicando explicitamente a ausência de referência.'
      : 'ATENÇÃO: Nenhum trecho relevante localizado na base BCA para esta consulta. Responda com conhecimento jurídico geral e informe que a resposta não tem origem nos documentos BCA.';

    const userContent = contextText
      ? `════════════════════════════════\nMODELOS BCA (base de referência do escritório):\n════════════════════════════════\n${contextText}\n\n════════════════════════════════\nDOCUMENTO ANALISADO (texto enviado para comparação):\n════════════════════════════════\n${message}`
      : `${semContexto}\n\nDOCUMENTO ANALISADO:\n${message}`;

    // 5. Chama Gemini
    const { texto: reply, status: geminiStatus, erro: geminiErro } = await chamarGemini(systemPrompt, userContent);

    const debugInfo = {
      modelo:        'gemini-2.5-flash',
      mode:          mode ?? 'chat',
      gemini_status: geminiStatus,
      has_reply:     !!reply,
      gemini_error:  geminiErro,
      chunks:        chunksDebug,
      context_chars: contextText.length,
    };

    if (!reply) {
      return responder({
        response: geminiStatus === 429
          ? 'O serviço está sobrecarregado. Aguarde alguns instantes e tente novamente.'
          : 'Sem resposta da IA. Verifique o campo debug.',
        debug: debugInfo,
      });
    }

    return responder({ response: reply, debug: debugInfo });

  } catch (err: any) {
    return responder({ error: err.message }, 500);
  }
});