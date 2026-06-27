import { Injectable } from '@angular/core';
import * as signalR from '@microsoft/signalr';
import { Subject, Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class TelemetryService {
  private hubConnection!: signalR.HubConnection;
  // A Subject acts as an event bus that can broadcast values to multiple dashboard components
  private riskAlertSubject = new Subject<any>();

  constructor() {
    this.startConnection();
  }

  private startConnection() {
    // 1. Point directly to the .NET SignalR Hub endpoint on port 5015
    this.hubConnection = new signalR.HubConnectionBuilder()
      .withUrl('http://localhost:5015/stream/alerts', {
        skipNegotiation: true,
        transport: signalR.HttpTransportType.WebSockets
      })
      .withAutomaticReconnect() // Forcing auto-reconnection mechanics if streaming cuts out
      .build();

    // 2. Start the WebSocket channel
    this.hubConnection
      .start()
      .then(() => console.log('⚡ SignalR WebSocket connection established seamlessly!'))
      .catch(err => console.error('💥 Error establishing SignalR handshake: ', err));

    // 3. Register listener for the backend webhook broadcast string
    this.hubConnection.on('ReceiveRiskAlert', (data) => {
      console.log('📥 Live risk stream payload captured: ', data);
      this.riskAlertSubject.next(data);
    });
  }

  // Expose an explicit method for the dashboard components to subscribe to
  getLiveAlerts(): Observable<any> {
    return this.riskAlertSubject.asObservable();
  }
}
