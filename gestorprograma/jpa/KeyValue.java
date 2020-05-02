package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.jpa;

import java.io.Serializable;

public class KeyValue implements Serializable
{

   private static final long serialVersionUID = 1166010653994302612L;

   private String key;

   private Object value;

   private String restriction;

   public KeyValue()
   {

   }

   public KeyValue(String key, Object value)
   {
      super();
      this.key = key;
      this.value = value;
   }

   /**
    * 
    * @param key Nombre de columna segun base de datos
    * @param value
    * @param restriction Simbolos permitidos
    */
   public KeyValue(String key, Object value, String restriction)
   {
      super();
      this.key = key;
      this.value = value;
      this.restriction = restriction;
   }

   /**
    * Nombre de columna segun base de datos
    * 
    * @return
    */
   public String getKey()
   {
      return key;
   }

   public void setKey(String key)
   {
      this.key = key;
   }

   public Object getValue()
   {
      return value;
   }

   public void setValue(Object value)
   {
      this.value = value;
   }

   public String getRestriction()
   {
      return restriction;
   }

   public void setRestriction(String restriction)
   {
      this.restriction = restriction;
   }
}
