# server.py
import asyncio
import grpc
import os
from concurrent import futures
from typing import AsyncIterable
from fastapi import FastAPI
import openai
from openai import AsyncOpenAI

import llm_pb2
import llm_pb2_grpc

openai.api_key = os.getenv("OPENAI_API_KEY")

# --- gRPC Servicer implementation ---
class LLMServicer(llm_pb2_grpc.LLMServiceServicer):
    async def StreamGenerate(self, request, context: grpc.aio.ServicerContext) -> AsyncIterable[llm_pb2.LLMResponse]:
        """
        Handle LLMRequest -> stream of LLMResponse tokens.
        """
        correlation_id = request.correlation_id

        # Combine contexts into one prompt
        system_prompt = (
            "You are an assistant that answers using the following context.\n\n"
            "VECTOR DB CONTEXT:\n" + "\n".join(request.vector_contexts) + "\n\n"
            "SERPER CONTEXT:\n" + "\n".join(request.serper_contexts) + "\n\n"
            "USER QUERY:\n" + request.user_query
        )

        client = AsyncOpenAI(api_key=openai.api_key)

        try:
            stream = await client.chat.completions.create(
                model=request.model_name or "gpt-4o-mini",
                messages=[{"role": "system", "content": system_prompt}],
                temperature=request.temperature or 0.2,
                max_tokens=request.max_tokens or 512,
                stream=True,
            )

            async for event in stream:
                if event.choices[0].delta and "content" in event.choices[0].delta:
                    token = event.choices[0].delta.content
                    yield llm_pb2.LLMResponse(
                        correlation_id=correlation_id,
                        token=token,
                        is_final=False,
                    )

            # send final marker
            yield llm_pb2.LLMResponse(
                correlation_id=correlation_id,
                token="",
                is_final=True,
            )

        except Exception as e:
            print(f"[Error] Generation failed: {e}")
            await context.abort(grpc.StatusCode.INTERNAL, str(e))


# --- Run both FastAPI + gRPC in one process ---
app = FastAPI(title="LLM Service")

@app.get("/health")
def health():
    return {"status": "ok"}


async def serve_grpc() -> None:
    server = grpc.aio.server()
    llm_pb2_grpc.add_LLMServiceServicer_to_server(LLMServicer(), server)
    listen_addr = "[::]:50051"
    server.add_insecure_port(listen_addr)
    print(f"gRPC server listening on {listen_addr}")
    await server.start()
    await server.wait_for_termination()


async def main():
    grpc_task = asyncio.create_task(serve_grpc())
    fastapi_task = asyncio.create_task(
        asyncio.to_thread(lambda: os.system("uvicorn server:app --port 8000 --host 0.0.0.0"))
    )
    await asyncio.gather(grpc_task, fastapi_task)


if __name__ == "__main__":
    asyncio.run(main())
