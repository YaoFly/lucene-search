package control;


class Hash {
    private static int[] table=createHashTable();
    private static final int HASH_OFFSET = 0;
    private static final int HASH_A = 1;
    private static final int HASH_B = 2;
    private static int[] createHashTable(){
        int[] newTable=new int[0x500];
        int seed = 0x00100001;
        int index1;
        int index2;
        int i;
        for( index1 = 0; index1 < 0x100; index1++ )
        {
            for( index2 = index1, i = 0; i < 5; i++, index2 += 0x100 )
            {
                int temp1, temp2;

                seed = (seed * 125 + 3) % 0x2AAAAB;
                temp1 = (seed & 0xFFFF) << 0x10;

                seed = (seed * 125 + 3) % 0x2AAAAB;
                temp2 = (seed & 0xFFFF);

                newTable[index2] = ( temp1 | temp2 );
            }
        }

        return newTable;
    }

    static private int innerHash(byte[] key,int type){
        int keyIdx  = 0;
        int seed1 = 0x7FED7FED;
        int seed2 = 0xEEEEEEEE;
        int ch;

        while( keyIdx<key.length )
        {
            ch = key[keyIdx];
            keyIdx+=1;
            seed1 = table[(type<<8) + ch] ^ (seed1 + seed2);
            seed2 = ch + seed1 + seed2 + (seed2<<5) + 3;
        }
        return Math.abs(seed1);

    }

    static int hashString(byte[] key)
    {
        return innerHash(key,HASH_OFFSET);
    }

    static int hashStringTypeA(byte[] key)
    {
        return innerHash(key,HASH_A);
    }

    static int hashStringTypeB(byte[] key)
    {
        return innerHash(key,HASH_B);
    }
}
