package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.jpa;


public class Item implements java.io.Serializable
{

   private static final long serialVersionUID = 7031274938466595823L;

   private String key;

   private String value;

   public Item()
   {

   }

   public Item(String key, String value)
   {
      super();
      this.key = key;
      this.value = value;
   }

   public String getKey()
   {
      return key;
   }

   public void setKey(String key)
   {
      this.key = key;
   }

   public String getValue()
   {
      return value;
   }

   public void setValue(String value)
   {
      this.value = value;
   }

   @Override
   public String toString()
   {
      StringBuilder builder = new StringBuilder();
      builder.append(key);
      builder.append(",");
      builder.append(value);
      return builder.toString();
   }

}
