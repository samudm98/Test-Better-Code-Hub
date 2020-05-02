package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.jpa;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;

import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.entity.InProgramaRegla;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.entity.InRegla;

public class InReglaDao extends JPAEntityDao
{

   public InReglaDao(EntityManager paramEntityManager)
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

   /**
    * Permite obtener las reglas vigentes ordenadas y asociadas al programa indicado para su ejecución.
    * 
    * @param idPrograma
    * @param fechaVigencia
    * @return
    */
   @SuppressWarnings("unchecked")
   public List<InRegla> obtenerReglasDelPrograma(String idPrograma, Date fechaVigencia)
   {
       
      String sqlInProgRegla = "SELECT ID, ID_PROGRAMA, ID_REGLA, NUM_ORDER FROM IN_PROGRAMA_REGLA WHERE ID_PROGRAMA = ? AND ? >= FEC_INI_VIGENCIA "
            + "AND ? <= FEC_FIN_VIGENCIA ORDER BY NUM_ORDER";

      
       //System.out.println("::ANDRES21:: idPrograma: " + idPrograma);
       //System.out.println("::ANDRES22:: fechaVigencia: " + fechaVigencia);
       //System.out.println("::ANDRES22:: sqlInProgRegla: " + sqlInProgRegla);
      
      
      Query query = getEntityManager().createNativeQuery(sqlInProgRegla, InProgramaRegla.class);
      query.setParameter(1, idPrograma);
      query.setParameter(2, fechaVigencia);
      query.setParameter(3, fechaVigencia);

      List<InProgramaRegla> listProgRegla = query.getResultList();
      
      
      //System.out.println("::ANDRES23:: size listProgRegla: " + listProgRegla.size());
      

      String sql = "SELECT * FROM IN_REGLA WHERE ID IN "
            + "(SELECT ID_REGLA FROM IN_PROGRAMA_REGLA WHERE ID_PROGRAMA = ? AND ? >= FEC_INI_VIGENCIA "
            + "AND ? <= FEC_FIN_VIGENCIA)";

      query = getEntityManager().createNativeQuery(sql, InRegla.class);
      query.setParameter(1, idPrograma);
      query.setParameter(2, fechaVigencia);
      query.setParameter(3, fechaVigencia);

      List<InRegla> listRegla = query.getResultList();
      
      
      //System.out.println("::ANDRES24:: size listRegla: " + listRegla.size());
      //System.out.println("::ANDRES24:: sql: " + sql);
      

      Map<String, InRegla> reglaMap = new HashMap<String, InRegla>();

      for (InRegla in : listRegla)
         reglaMap.put(in.getId(), in);

      List<InRegla> result = new ArrayList<InRegla>();

      for (InProgramaRegla obj : listProgRegla)
         result.add(reglaMap.get(obj.getIdRegla()));

      
      /*
      for (InRegla obj : result)
      {    
          System.out.println("::ANDRES34:: InRegla getNombre:" + obj.getNombre());
      }
      */
      
      
      return result;
   }

}
