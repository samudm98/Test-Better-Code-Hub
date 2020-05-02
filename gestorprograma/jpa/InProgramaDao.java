package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.jpa;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;

public class InProgramaDao extends JPAEntityDao
{

   public InProgramaDao(EntityManager paramEntityManager)
   {
      super(paramEntityManager);
   }

   @SuppressWarnings({ "unchecked", "rawtypes" })
   public <E> List<E> customSearch(CustomSearch<E> search)
   {
      CriteriaBuilder criteriaBuilder = getEntityManager().getCriteriaBuilder();
      CriteriaQuery<E> criteriaQuery = (CriteriaQuery<E>) criteriaBuilder.createQuery(search.getClass());
      Root pRoot = criteriaQuery.from(search.getEntity().getClass());

      criteriaQuery.select(pRoot);

      if (search.getOrderBy() != null)
      {
         if (search.getOrderAsc() != null)
         {
            if (search.getOrderAsc())
               criteriaQuery.orderBy(criteriaBuilder.asc(pRoot.get(search.getOrderBy())));
            else
               criteriaQuery.orderBy(criteriaBuilder.desc(pRoot.get(search.getOrderBy())));
         }
      }

      if (search.getFilterKeys() != null && !search.getFilterKeys().isEmpty())
      {
         for (KeyValue obj : search.getFilterKeys())
         {
            if (">=".equals(obj.getRestriction()))
            {
               // Predicate predicate = criteriaBuilder.ge(pRoot.get(obj.getKey()), );
               // criteriaQuery.where(predicate);
            }
            else if ("<=".equals(obj.getRestriction()))
            {
               // criteriaQuery.add(Restrictions.le(obj.getKey(), obj.getValue()));
            }
            else if (">".equals(obj.getRestriction()))
            {
               // criteriaQuery.add(Restrictions.gt(obj.getKey(), obj.getValue()));
            }
            else if ("<".equals(obj.getRestriction()))
            {
               // criteriaQuery.add(Restrictions.lt(obj.getKey(), obj.getValue()));
            }
            else if ("=".equals(obj.getRestriction()))
               criteriaQuery.where(criteriaBuilder.equal(pRoot.get(obj.getKey()), obj.getValue()));
            else if ("<>".equals(obj.getRestriction()))
            {
               // criteriaQuery.add(Restrictions.ne(obj.getKey(), obj.getValue()));
            }
         }

      }

      return getEntityManager().createQuery(criteriaQuery).getResultList();

   }
}
