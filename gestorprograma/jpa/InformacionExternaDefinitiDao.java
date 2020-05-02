package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.jpa;

import co.gov.ugpp.parafiscales.servicios.liquidador.entity.InformacionExternaDefiniti;
import javax.persistence.EntityManager;
import javax.persistence.Query;

public class InformacionExternaDefinitiDao extends JPAEntityDao
{


   public InformacionExternaDefinitiDao(EntityManager paramEntityManager)
   {
      super(paramEntityManager);
   }


   public InformacionExternaDefiniti informacionExternaDefinitiPorRadicado(String radicado)
   {

      try 
      {
        String sql = "SELECT * FROM INFORMACION_EXTERNA_DEFINITI WHERE ID_RADICADO = ? ORDER BY FEC_CREACION DESC";  
          
        Query query = getEntityManager().createNativeQuery(sql, InformacionExternaDefiniti.class);
        query.setParameter(1, radicado);
        query.setMaxResults(1);

        InformacionExternaDefiniti informacion = (InformacionExternaDefiniti) query.getSingleResult();

        if (informacion != null)
           return informacion;
      } 
      catch (Exception ex) 
      {
        ex.printStackTrace();
        return null;
      } 
        
      return null;

   }
 
}