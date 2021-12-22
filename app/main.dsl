import "commonReactions/all.dsl";

context {
    input phone: string;
}

start node root
{
    do
    {
        #connectSafe($phone);
        #waitForSpeech(1000);
        #sayText("Добро пожаловать в почтовый сервис. Чем я могу вам помочь?");
        wait *;
    }
    transitions
    {
        track_parcel: goto track_parcel on #messageHasIntent("track_parcel");
        missed_delivery: goto missed_delivery on #messageHasIntent("missed_delivery");
        where_is_point: goto where_is_point on #messageHasIntent("where_is_point");
        return_shipment: goto return_shipment on #messageHasIntent("return_shipment");
    }
}
node track_parcel
{
    do
    {
        #sayText("К сожалению, функция отслеживания посылки ещё не реализована.");
        exit;
    }
    transitions
    {
    }
}
node missed_delivery
{
    do
    {
        #sayText("К сожалению, функция перезаписи доставки посылки ещё не реализована.");
        exit;
    }
    transitions
    {
    }
}
node where_is_point
{
    do
    {
        #sayText("К сожалению, функция отслеживания посылки не реализована.");
        exit;
    }
    transitions
    {
    }
}
node return_shipment
{
    do
    {
        #sayText("К сожалению, функция отмены доставки ещё не реализована.");
        exit;
    }
    transitions
    {
    }
}
