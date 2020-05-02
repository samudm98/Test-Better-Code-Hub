package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.jpa;

import java.io.Serializable;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.criteria.CriteriaQuery;

public class JPAEntityDao implements Serializable
{

   private EntityManager entityManager;

   public JPAEntityDao(EntityManager paramEntityManager)
   {
      this.entityManager = paramEntityManager;
   }

   public void setEntityManager(EntityManager paramEntityManager)
   {
      this.entityManager = paramEntityManager;
   }

   public EntityManager getEntityManager()
   {
      return this.entityManager;
   }

   public <E> E save(E paramE)
   {
      save(paramE, false);
      return paramE;
   }

   public <E> E save(E paramE, boolean paramBoolean)
   {
      getEntityManager().persist(paramE);
      //getEntityManager().merge(paramE);
      if (!(paramBoolean))
         return paramE;
      //getEntityManager().flush();
      return paramE;
   }

   public <E> void delete(E paramE, boolean paramBoolean)
   {
      getEntityManager().remove(paramE);
      if (!(paramBoolean))
         return;
      //getEntityManager().flush();
   }

   public <E> void delete(E paramE)
   {
      getEntityManager().remove(paramE);
   }

   public <E> E findById(Class<E> paramClass, Object paramInt)
   {
      return getEntityManager().find(paramClass, paramInt);
   }

   @SuppressWarnings({ "unchecked", "rawtypes" })
   public <E> List<E> findAll(Class<E> entityClass)
   {
      final CriteriaQuery cq = this.getEntityManager().getCriteriaBuilder().createQuery();
      cq.select(cq.from(entityClass));

      return this.getEntityManager().createQuery(cq).getResultList();
   }

   public <E> E update(E paramE)
   {
      save(paramE);
      return paramE;
   }

   public <E> E update(E paramE, boolean paramBoolean)
   {
      save(paramE, paramBoolean);
      return paramE;
   }

}
