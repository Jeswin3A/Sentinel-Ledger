import { TestBed } from '@angular/core/testing';

import { Telemetry } from './telemetry';

describe('Telemetry', () => {
  let service: Telemetry;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(Telemetry);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
