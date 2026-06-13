import time
import requests


class PNCPClient:
    """Cliente HTTP para a API de Consultas do PNCP, com retry e tratamento de erros."""

    def __init__(self, base_url: str, timeout: tuple[int, int] = (10, 15), tentativas: int = 3):
        self.base_url = base_url
        self.timeout = timeout
        self.tentativas = tentativas
        self._session = requests.Session()

    def fetch_page(self, params: dict) -> dict:
        pagina = params.get("pagina")
        last_exc: Exception | None = None

        for tentativa in range(1, self.tentativas + 1):
            try:
                response = self._session.get(self.base_url, params=params, timeout=self.timeout)
                response.raise_for_status()
                try:
                    return response.json()
                except ValueError:
                    return {}
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                last_exc = e
                print(f"  [TIMEOUT/REDE] pagina {pagina} (tentativa {tentativa}/{self.tentativas}): {e}")
                if tentativa < self.tentativas:
                    espera = 10 * tentativa
                    print(f"  aguardando {espera}s...")
                    time.sleep(espera)
            except requests.exceptions.HTTPError as e:
                status = e.response.status_code if e.response is not None else 0
                if status < 500:
                    print(f"  [ERRO HTTP {status}] pagina {pagina}: {e.response.text[:200] if e.response is not None else ''}")
                    return {}
                last_exc = e
                print(f"  [ERRO HTTP {status}] pagina {pagina}: {e}")
                if tentativa < self.tentativas:
                    time.sleep(10 * tentativa)
            except requests.exceptions.RequestException as e:
                last_exc = e
                print(f"  [ERRO REDE] pagina {pagina} (tentativa {tentativa}/{self.tentativas}): {e}")
                if tentativa < self.tentativas:
                    time.sleep(5 * tentativa)

        raise last_exc


class ContratacoesExtractor:
    """Extrator de contratações do PNCP: monta os parâmetros de busca e percorre a paginação."""

    def __init__(self, client: PNCPClient, modalidade: int, tamanho: int = 50):
        self.client = client
        self.modalidade = modalidade
        self.tamanho = tamanho

    def _build_params(self, data_inicial: str, data_final: str, uf: str | None, pagina: int) -> dict:
        params = {
            "dataInicial": data_inicial,
            "dataFinal": data_final,
            "codigoModalidadeContratacao": self.modalidade,
            "pagina": pagina,
            "tamanhoPagina": self.tamanho,
        }
        if uf:
            params["uf"] = uf.upper()
        return params

    def iter_pages(self, data_inicial: str, data_final: str, uf: str | None = None, delay_segundos: float = 1.5):
        primeira = self.client.fetch_page(self._build_params(data_inicial, data_final, uf, 1))
        if not primeira:
            return

        total_paginas = primeira.get("totalPaginas", 1)
        total_registros = primeira.get("totalRegistros", 0)
        print(f"Total de registros: {total_registros} | Total de paginas: {total_paginas}")

        yield 1, total_paginas, primeira.get("data", [])

        for pagina in range(2, total_paginas + 1):
            time.sleep(delay_segundos)
            resultado = self.client.fetch_page(self._build_params(data_inicial, data_final, uf, pagina))
            yield pagina, total_paginas, resultado.get("data", [])
