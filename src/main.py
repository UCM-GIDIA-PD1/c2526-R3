from extraccion import construccion_df, vegetacion, pendiente, fisicas
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

async def main():
    load_dotenv()
    incendios = os.getenv('INCENDIOS')
    #df_final = await construccion_df.build_environmental_df(incendios, limit = 50)
    #df_final = await vegetacion.df_vegetacion(incendios)
    #df_final = await pendiente.df_pendiente(incendios)
    df_final = await fisicas.df_fisicas(incendios)

if __name__ == "__main__":
    asyncio.run(main())
