package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.jpa;

import co.gov.ugpp.parafiscales.servicios.liquidador.entity.HojaCalculoLiquidacion;
import javax.persistence.EntityManager;
import javax.persistence.Query;


public class HojaCalculoLiquidacionDao extends JPAEntityDao {

    
  
   public HojaCalculoLiquidacionDao(EntityManager paramEntityManager)
   {
      super(paramEntityManager);
   }
   
   
   public HojaCalculoLiquidacion hojaCalculoLiquidacionPorId(String id)
   {

      try 
      {
        String sql = "SELECT * FROM LIQ_HOJA_CALCULO_LIQUIDACION WHERE ID = ?";  
          
        Query query = getEntityManager().createNativeQuery(sql, HojaCalculoLiquidacion.class);
        query.setParameter(1, id);
        query.setMaxResults(1);

        HojaCalculoLiquidacion informacion = (HojaCalculoLiquidacion) query.getSingleResult();

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
