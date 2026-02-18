from extraccion import construccion_df, vegetacion
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

async def main():
    load_dotenv()
    incendios = os.getenv('INCENDIOS')
    #df_final = await construccion_df.build_environmental_df(incendios, limit = 50)
    df_final = await vegetacion.df_vegetacion(incendios)

if __name__ == "__main__":
    asyncio.run(main())
