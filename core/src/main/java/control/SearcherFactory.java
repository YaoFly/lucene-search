package control;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

class SearcherFactory
        extends BasePooledObjectFactory<PooledSearcher>{

    private int poolId=-1;

    SearcherFactory(int pid){
        super();
        poolId=pid;
    }

    @Override
    public PooledSearcher create() {
        RTIndex ir=SearchGlobal.getInstance().getShardIndex(poolId);
        if(ir!=null) {
            return new PooledSearcher(
                    SearchGlobal.getInstance().getMaxResults(),
                    ir,
                    poolId);
        }
        return null;
    }

    @Override
    public PooledObject<PooledSearcher> wrap(PooledSearcher buffer) {
        return new DefaultPooledObject<>(buffer);
    }

    @Override
    public void passivateObject(PooledObject<PooledSearcher> pooledObject) {
        //do nothing
    }

    @Override
    public void activateObject(PooledObject<PooledSearcher> p){
        //do nothing
    }
}
